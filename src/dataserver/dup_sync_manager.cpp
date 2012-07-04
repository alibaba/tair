/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * duplicate_manager.cpp is to performe the duplicate func when copy_count > 1
 *
 * Version: $Id: dup_sync_manager.cpp 28 2011-09-19 05:18:09Z xinshu.wzx@taobao.com $
 *
 * Authors:
 *   xinshu <xinshu.wzx@taobao.com>
 *
 */
#include "dup_sync_manager.hpp"
#include "syncproc.hpp"
#include "packet_factory.hpp"
#include "packet_streamer.hpp"
#include "inc_dec_packet.hpp"
#include "storage_manager.hpp"
namespace tair{

  CPacket_wait_manager::CPacket_wait_manager()
  {
    m_slots_locks = new CSlotLocks(MAP_BUCKET_SLOT);
    for (int i = 0; i < TAIR_MAX_BUCKET_NUMBER; i++)
    {
      m_bucket_count[i] = 0;
    }
  }

  CPacket_wait_manager::~CPacket_wait_manager()
  {
    if (m_slots_locks)
    {
      delete m_slots_locks;
      m_slots_locks = NULL;
    }
  }

  bool CPacket_wait_manager::isBucketFree(int bucket_number)
  {
    assert(bucket_number >= 0 && bucket_number < TAIR_MAX_BUCKET_NUMBER);
    CScopedRwLock __scoped_lock(m_slots_locks->getlock(0), false);
    return m_bucket_count[bucket_number] < MAX_BUCKET_FREE_COUNT;
  }

  int CPacket_wait_manager::changeBucketCount(int bucket_number, int number)
  {
    //dont't lock it. should lock outside
    assert(bucket_number >= 0 && bucket_number < TAIR_MAX_BUCKET_NUMBER);
    m_bucket_count[bucket_number] += number; //-1,or +1
    if (m_bucket_count[bucket_number] < 0) m_bucket_count[bucket_number] = 0;
    return 0;
  }

  int CPacket_wait_manager::get_map_index(uint32_t max_packet_id)
  {
    return 0; //must be zero,bucause of number indictor.
    //int index=max_packet_id%MAP_BUCKET_SLOT;
    //assert(index>= 0 && index<MAP_BUCKET_SLOT);
    //return index;
  }

  //the index must 0-2,already checked.
  bool CPacket_wait_manager::put_timeout_hint(int index, CPacket_Timeout_hint *phint)
  {
    return dup_wait_queue[index].put(phint);
  }

  CPacket_Timeout_hint *CPacket_wait_manager::get_timeout_hint(int index, int msec)
  {
    return dup_wait_queue[index].get(msec);
  }

  int CPacket_wait_manager::addWaitNode(int area, const data_entry*, const data_entry* value, int bucket_number,
      const vector<uint64_t>& des_server_ids, base_packet* request, uint32_t max_packet_id, int &version)
  {
    //check map_size first
    int index = get_map_index(max_packet_id);
    {
      //CRwLock m_lock(m_mutex,RLOCK);
      CScopedRwLock __scoped_lock(m_slots_locks->getlock(index), false);
      if(m_PkgWaitMap[index].size() > TAIR_MAX_DUP_MAP_SIZE) return TAIR_RETURN_DUPLICATE_BUSY;
    }

    //CRwLock m_lock(m_mutex,WLOCK);
    CScopedRwLock __scoped_lock(m_slots_locks->getlock(index), true);
    CDuplicatPkgMapIter itr = m_PkgWaitMap[index].find(max_packet_id);
    if (itr == m_PkgWaitMap[index].end())
    {
      //not found in map .inert it and inster to a queue.
      CPacket_wait_Nodes *pdelaysign = new CPacket_wait_Nodes(bucket_number, request, des_server_ids, version, value);
      changeBucketCount(bucket_number, 1);
      m_PkgWaitMap[index][max_packet_id] = pdelaysign;
      return TAIR_RETURN_SUCCESS;
    }
    else
    {
      //should never happen,but if crash and restared, nay mixed.
      log_error("packet sequnce id is dup");
      return TAIR_RETURN_DUPLICATE_IDMIXED;
    }
  }

  int CPacket_wait_manager::doResponse(int bucket_number, uint64_t des_server_id, uint32_t max_packet_id,
      struct CPacket_wait_Nodes **ppNode)
  {
    //CRwLock m_lock(m_mutex,WLOCK);
    int index = get_map_index(max_packet_id);
    CScopedRwLock __scoped_lock(m_slots_locks->getlock(index), true);

    CDuplicatPkgMapIter itr = m_PkgWaitMap[index].find(max_packet_id);
    if (itr != m_PkgWaitMap[index].end())
    {
      int ret=itr->second->do_response(bucket_number,des_server_id);
      if (0 == ret)
      {
        changeBucketCount(bucket_number, -1);
        *ppNode= itr->second;
        m_PkgWaitMap[index].erase(itr);
      }
      else
      {
        *ppNode= NULL;
      }

      return ret;
    }
    else
    {
      //already timeout.
      log_warn("resonse packet %u, but not found", max_packet_id);
	    *ppNode= NULL;
      return TAIR_RETURN_DUPLICATE_DELAY;
    }
  }

  int CPacket_wait_manager::doTimeout( uint32_t max_packet_id)
  {
    //CRwLock m_lock(m_mutex,WLOCK);
    int index = get_map_index(max_packet_id);
    {
      CScopedRwLock __scoped_lock(m_slots_locks->getlock(index), false);
      CDuplicatPkgMapIter itr = m_PkgWaitMap[index].find(max_packet_id);
      if (itr == m_PkgWaitMap[index].end()) return 0;
    }
    //now we should clear it.
    clear_waitnode(max_packet_id);
    return TAIR_RETURN_DUPLICATE_ACK_TIMEOUT;
  }

  int CPacket_wait_manager::clear_waitnode( uint32_t max_packet_id)
  {
    int index=get_map_index(max_packet_id);
    CScopedRwLock __scoped_lock(m_slots_locks->getlock(index), true);

    CDuplicatPkgMapIter itr = m_PkgWaitMap[index].find(max_packet_id);
    if (itr != m_PkgWaitMap[index].end())
    {
      CPacket_wait_Nodes* pNode = itr->second;
      changeBucketCount(pNode->bucket_number, -1);
      m_PkgWaitMap[index].erase(itr);
      delete pNode; pNode = NULL;
      return 0;
    }
    return 0;
  }

  dup_sync_sender_manager::dup_sync_sender_manager( tbnet::Transport *transport,
      tair_packet_streamer *streamer, table_manager* table_mgr)
  {
    this->table_mgr = table_mgr;
    conn_mgr = new tbnet::ConnectionManager(transport, streamer, this);
    conn_mgr->setDefaultQueueTimeout(0 , MISECONDS_BEFOR_SEND_RETRY/2000);
    max_queue_size = 0;
    atomic_set(&packet_id_creater, 0);
    setThreadCount(MAX_DUP_COUNT);
    //this->start();
  }

  dup_sync_sender_manager::~dup_sync_sender_manager()
  {
    this->stop();
    this->wait();
    delete conn_mgr;
  }

  void dup_sync_sender_manager::do_hash_table_changed()
  {
    return ;
  }

  bool dup_sync_sender_manager::is_bucket_available(uint32_t bucket_number)
  {
    return true;
  }

  //xinshu. add new function for directy duplicate.
  int dup_sync_sender_manager::duplicate_data(int area, const data_entry* key, const data_entry* value, int expire_time,
      int bucket_number, const vector<uint64_t>& des_server_ids, base_packet *request, int version)
  {
    if (des_server_ids.empty()) return 0;

    if (!request) return 0;
    //first keep it in queue. until timeout it.
    unsigned int _copy_count = des_server_ids.size();
    uint32_t max_packet_id = atomic_add_return(_copy_count, &packet_id_creater);

    //and keep it in map . until response arrive.
    log_debug("wait bucket %d, packet %d, timeout=%d", bucket_number, max_packet_id, expire_time);
    int ret = packets_mgr.addWaitNode(area, key, value, bucket_number, des_server_ids, request, max_packet_id, version);
    if (ret!=0)
    {
      log_error("addWaitNode error, ret=%d\n",ret);
      return ret;
    }
    ret = direct_send(area, key, value, 0, bucket_number, des_server_ids, max_packet_id);
    if (TAIR_RETURN_SUCCESS != ret)
    {
      //clear waitnode.
      packets_mgr.clear_waitnode(max_packet_id);
      //return failed,the caller will delete the packet.
      return ret;
    }

    return TAIR_DUP_WAIT_RSP;
  }

  int dup_sync_sender_manager::direct_send(int area, const data_entry* key, const data_entry* value, int expire_time,
      int bucket_number, const vector<uint64_t>& des_server_ids, uint32_t max_packet_id)
  {
    unsigned _copy_count = des_server_ids.size();
    //? what if `max_packet_id` is rounded up to 0, up there in `duplicate_data`
    if (0 == max_packet_id)
      max_packet_id = atomic_add_return(_copy_count, &packet_id_creater);

    for(unsigned int i = 0; i < _copy_count; i++)
    {
      //new a dup packet
      request_duplicate* tmp_packet = new request_duplicate();
      tmp_packet->packet_id = max_packet_id;
      tmp_packet->area = area;
      tmp_packet->key = *key;
      tmp_packet->key.server_flag = TAIR_SERVERFLAG_DUPLICATE;
      if (tmp_packet->key.is_alloc() == false) {
        tmp_packet->key.set_data(key->get_data(), key->get_size(), true);
      }
      if (value) {
        tmp_packet->data = *value;
        if (tmp_packet->data.is_alloc() == false) {
          tmp_packet->data.set_data(value->get_data(), value->get_size(), true);
        }
      } else { //remove
        tmp_packet->data.data_meta.flag = TAIR_ITEM_FLAG_DELETED;
      }
      tmp_packet->bucket_number = bucket_number;

      //and send it to slave
      log_debug("duplicate packet %d sent: %s", tmp_packet->packet_id,tbsys::CNetUtil::addrToString(des_server_ids[i]).c_str());
      if (conn_mgr->sendPacket(des_server_ids[i], tmp_packet, NULL, (void*)((long)max_packet_id), true) == false)
      {
        //duplicate sendpacket error.
        log_error("duplicate packet %d faile send: %s",
            tmp_packet->packet_id,tbsys::CNetUtil::addrToString(des_server_ids[i]).c_str());
        delete tmp_packet;
        return TAIR_RETURN_DUPLICATE_BUSY;
      }
    }
    return TAIR_RETURN_SUCCESS;
  }

  int dup_sync_sender_manager::duplicate_data(int32_t bucket_number, request_prefix_puts *request, vector<uint64_t> &slaves, int version)
  {
    return do_duplicate_data(bucket_number, request, slaves, version);
  }

  int dup_sync_sender_manager::duplicate_data(int32_t bucket_number, request_prefix_removes *request, vector<uint64_t> &slaves, int version)
  {
    return do_duplicate_data(bucket_number, request, slaves, version);
  }

  int dup_sync_sender_manager::duplicate_data(int32_t bucket_number, request_prefix_hides *request, vector<uint64_t> &slaves, int version)
  {
    return do_duplicate_data(bucket_number, request, slaves, version);
  }

  int dup_sync_sender_manager::duplicate_data(int32_t bucket_number, request_prefix_incdec *request, vector<uint64_t> &slaves, int version)
  {
    return do_duplicate_data(bucket_number, request, slaves, version);
  }

  bool dup_sync_sender_manager::has_bucket_duplicate_done(int bucketNumber)
  {
    /**
      while(packages_mgr_mutex.rdlock()) {
      usleep(10);
      }
      map<uint32_t, bucket_waiting_queue>::iterator it = packets_mgr.find(bucketNumber);
      bool res = (it == packets_mgr.end() || it->second.size() == 0 ) ;
      packages_mgr_mutex.unlock();
      return res;
     **/
    return packets_mgr.isBucketFree(bucketNumber);
  }

  int dup_sync_sender_manager::do_duplicate_response(uint32_t bucket_number, uint64_t d_serverId, uint32_t packet_id)
  {
	   CPacket_wait_Nodes *pNode=NULL;
    int ret = packets_mgr.doResponse(bucket_number, d_serverId, packet_id,  &pNode);
    //if all rsp arrive ,resp to client. or get a TAIR_RETURN_DUPLICATE_ACK_WAIT.
    if (0 == ret && pNode)
    {
      ret = rspPacket(pNode);
	    delete (pNode);(pNode)=NULL;
    }
    return ret;
  }

  int dup_sync_sender_manager::rspPacket(const CPacket_wait_Nodes * pNode)
  {
    int rv = 0;
    switch  (pNode->pcode)
    {
      case TAIR_REQ_PUT_PACKET:
      case TAIR_REQ_REMOVE_PACKET:
      case TAIR_REQ_LOCK_PACKET:
        rv = tair_packet_factory::set_return_packet(pNode->conn,pNode->chid,pNode->pcode,0,"",pNode->conf_version);
        break;
      case TAIR_REQ_INCDEC_PACKET:
        {
          response_inc_dec *resp = new response_inc_dec();
          resp->value = pNode->inc_value_result ;
          resp->config_version = pNode->conf_version;
          resp->setChannelId(pNode->chid);
          if (pNode->conn->postPacket(resp) == false)
          {
            delete resp;
            rv = TAIR_RETURN_DUPLICATE_SEND_ERROR;
          }
        }
        break;
      default:
        log_warn("unknow handle error! get pcode =%d!", pNode->pcode);
        //rv=tair_packet_factory::set_return_packet(request, 0, "", conf_version);
        break;
    };
    return rv;
  }

  void dup_sync_sender_manager::run(tbsys::CThread *thread, void *arg)
  {
    UNUSED(thread);
    UNUSED(arg);
    return;
    while (!_stop)
    {
      try
      {
        //int index = (long)arg;//if need more thread,shoudld hash it.
      }
      catch(...)
      {
        log_warn("unknow error! get timeoutqueue error!");
      }
    }
  }

  void dup_sync_sender_manager::handleTimeOutPacket(uint32_t packet_id)
  {
    //not exist or timeout.
    packets_mgr.doTimeout(packet_id);
    //todo:if timeout,should response to client or rollback?

    //now i should free the request,not need convert to child class.
  }

  tbnet::IPacketHandler::HPRetCode dup_sync_sender_manager::handlePacket(tbnet::Packet *packet, void *args)
  {
	  int packet_id = reinterpret_cast<long>(args);
    if (!packet->isRegularPacket()) {
      tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
			log_error("cmd:%d,packetid:%d,timeout.", cp->getCommand(),packet_id);
		  handleTimeOutPacket(packet_id);
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }
    int pcode = packet->getPCode();
    log_debug("================= get duplicate response, pcode: %d", pcode);
    if (TAIR_RESP_DUPLICATE_PACKET == pcode) {
      response_duplicate* resp = (response_duplicate*)packet;
      log_debug("response packet %d ,bucket =%d, server=%s",resp->packet_id,resp->bucket_id, \
          tbsys::CNetUtil::addrToString(resp->server_id).c_str());
      int ret = do_duplicate_response(resp->bucket_id, resp->server_id, resp->packet_id);
      if (0 != ret && TAIR_RETURN_DUPLICATE_ACK_WAIT != ret)
      {
        log_error("response packet %d failed,bucket =%d, server=%s, code=%d", resp->packet_id, resp->bucket_id, \
            tbsys::CNetUtil::addrToString(resp->server_id).c_str(), ret);
      }

      resp->free();
    } else if ( pcode == TAIR_RESP_MRETURN_DUP_PACKET) {
      response_mreturn_dup *resp_dup = dynamic_cast<response_mreturn_dup*>(packet);
      if (resp_dup == NULL) {
        log_error("bad packet %d", pcode);
      } else {
        log_debug("duplicate response packet %u, bucket = %d, server = %s", resp_dup->packet_id, resp_dup->bucket_id, tbsys::CNetUtil::addrToString(resp_dup->server_id).c_str());
        CPacket_wait_Nodes *pnode = NULL;
        int ret = packets_mgr.doResponse(resp_dup->bucket_id, resp_dup->server_id, resp_dup->packet_id, &pnode);
        response_mreturn *resp = new response_mreturn();
        resp->swap(*resp_dup);
        if (ret == TAIR_RETURN_SUCCESS && pnode != NULL) {
          resp->setChannelId(pnode->chid);
          resp->config_version = pnode->conf_version;
          if (pnode->conn->postPacket(resp) == false) {
            delete resp;
            ret = TAIR_RETURN_DUPLICATE_SEND_ERROR;
          }
        } else {
          delete resp;
        }
      }
    } else if (pcode == TAIR_RESP_PREFIX_INCDEC_PACKET) {
      response_prefix_incdec *resp = dynamic_cast<response_prefix_incdec*>(packet);
      if (resp == NULL) {
        log_error("bad packet %d", pcode);
      } else {
        log_debug("duplicate response packet %u, bucket = %d, server = %s",
            resp->packet_id, resp->bucket_id, tbsys::CNetUtil::addrToString(resp->server_id).c_str());
        CPacket_wait_Nodes *pnode = NULL;
        int ret = packets_mgr.doResponse(resp->bucket_id, resp->server_id, resp->packet_id, &pnode);
        if (ret == TAIR_RETURN_SUCCESS && pnode != NULL) {
          resp->server_flag = TAIR_SERVERFLAG_CLIENT;
          resp->setChannelId(pnode->chid);
          resp->config_version = pnode->conf_version;
          if (!pnode->conn->postPacket(resp)) {
            delete resp;
            ret = TAIR_RETURN_DUPLICATE_SEND_ERROR;
          }
        }
      }
    } else {
      log_warn("unknow packet! pcode: %d", pcode);
      packet->free();
    }

    return tbnet::IPacketHandler::KEEP_CHANNEL;
  }
}
