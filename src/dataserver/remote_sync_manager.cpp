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
#include "remote_sync_manager.hpp"
#include "syncproc.hpp"
#include "packet_factory.hpp"
#include "inc_dec_packet.hpp"

#define MAX_SLOW_LIST_SIZE  MAX_SYNC_LIST_SIZE*2  
#define REMOTE_CONF_NAME "etc/remote.conf"


namespace tair{

   enum {
      REMOTE_LOCK_INT= 0,
      REMOTE_LOCK_SENEDMAP=1,
      REMOTE_LOCK_MAX=2,
   };

   remote_sync_manager::remote_sync_manager(tair_manager *tair_mgr,const char *base_home)
   {
     this->tair_mgr = tair_mgr;
     atomic_set(&packet_id_creater, 0);
     m_slots_locks =new  CSlotLocks(REMOTE_LOCK_MAX);
     strncpy(m_base_home,base_home,NAME_MAX);  //base_home never null
     memset(m_data_dir,0,NAME_MAX);
     m_b_use_disk=false;
     m_inited=false;
   }


   bool remote_sync_manager::init()
   {
     //case reinit;
     if(m_inited) return m_inited;
     CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_INT),true);
     if(m_inited) return m_inited;

     //read file from config file.
     char remote_file_name[NAME_MAX];
     snprintf(remote_file_name,NAME_MAX,"%s/%s",m_base_home,REMOTE_CONF_NAME);

     tbsys::CConfig remote_config;
     if(remote_config.load((char *)remote_file_name) == EXIT_FAILURE) 
     {
       log_error("load sync config file %s error", remote_file_name);
       return false;
     }
     const char *remote_data_dir=remote_config.getString(REMOTE_TAIRSERVER_SECTION,REMOTE_DATA_DIR,REMOTE_DEFAULT_DATA_DIR);

     if(remote_data_dir)
     {
       sprintf(m_data_dir,"%s/%s",m_base_home,remote_data_dir); //keep for rotate
       if(!tbsys::CFileUtil::mkdirs(m_data_dir)) 
       {
         log_error("sync config mkdir datadir %s error", remote_data_dir);
         return false;
       }
       char binlog[NAME_MAX];
       if(!m_tosend_queue.enable_disk_save(remote_data_dir)) 
       {
         log_error("open data file failed %s",remote_data_dir);
         return false;
       }

       //set failed log key file.
       snprintf(binlog,255,"%s/binlog.%d",remote_data_dir,getpid());
       if(!m_sync_log_file.start(binlog,false))
       {
         log_error("open binlog file %s falied ",binlog);
         return false;
       }
       m_b_use_disk=true;
     }

     //init remote tairclient.
     const char *remote_master= remote_config.getString(REMOTE_TAIRSERVER_SECTION, TAIR_MASTER_NAME,NULL);
     const char *remote_slave= remote_config.getString(REMOTE_TAIRSERVER_SECTION, TAIR_SLAVE_NAME, NULL);
     const char *remote_group= remote_config.getString(REMOTE_TAIRSERVER_SECTION, TAIR_GROUP_NAME, NULL);

     if(!remote_master||!remote_group) 
     {
       log_error("sync config not master or group in %s",remote_file_name);
       return false;
     }
     log_info("create remote sync with  %s:%s",remote_master,remote_group);
     m_remote_tairclient.set_timeout(2000);
     if(!m_remote_tairclient.startup(remote_master,remote_slave, remote_group))
     {
       log_error("remote:%s,%s can not connect,init failed",remote_master,remote_group);
       return false;
     }
     else
     {
       log_info("sync to remote:%s,%s ok.",remote_master,remote_group);
     }

     setThreadCount(2);
     this->start();
     m_inited=true;
     return true;
   }

   bool remote_sync_manager::init_local_client()
   {
     CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_INT),true);
     if(m_local_tairclient.isinited()) return true;

     //read file from config file.
     char remote_file_name[NAME_MAX];
     snprintf(remote_file_name,NAME_MAX,"%s/%s",m_base_home,REMOTE_CONF_NAME);

     tbsys::CConfig remote_config;
     if(remote_config.load((char *)remote_file_name) == EXIT_FAILURE) 
     {
       log_error("load sync config file %s error", remote_file_name);
       return false;
     }
     //init local_tairclient.
     const char *local_master =remote_config.getString(LOCAL_TAIRSERVER_SECTION, TAIR_MASTER_NAME, NULL);
     const char *local_slave  =remote_config.getString(LOCAL_TAIRSERVER_SECTION, TAIR_SLAVE_NAME, NULL);
     const char *local_group  =remote_config.getString(LOCAL_TAIRSERVER_SECTION, TAIR_GROUP_NAME, NULL);
     if(!local_master||!local_slave) 
     {
       log_error("sync config no local master or group in %s",remote_file_name);
       return false;
     }
     else
     {
       m_local_tairclient.set_timeout(2000);
       if(!m_local_tairclient.startup(local_master ,local_slave, local_group))
       {
         log_error("local:%s,%s can not connect ",local_master ,local_group);
         return false;
       }
       return true;
     }
   }


   remote_sync_manager::~remote_sync_manager()
   {
      this->stop();
      this->wait();
      delete  m_slots_locks;
   }

   int remote_sync_manager::sync_put_data(int area,const data_entry& key, const data_entry& value,int expired, int version)
   {
     //now  will just put the data into lists.
     if(!m_inited) return  TAIR_RETURN_REMOTE_NOTINITED;
     request_sync * pnode= new  request_sync(TAIR_REQ_PUT_PACKET,area,key,value,expired,version);
     m_tosend_queue.put(pnode);
     return 0;
   }

   int remote_sync_manager::sync_remove_data(int area,const data_entry& key)
   {
     if(!m_inited) return  TAIR_RETURN_REMOTE_NOTINITED;
     request_sync * pnode= new  request_sync(TAIR_REQ_REMOVE_PACKET,area,key);
     m_tosend_queue.put(pnode);
     return 0;
   }

   void remote_sync_manager::callback_sync_response(int error_code, void * arg)
   {
     //tairclient start single thread do it.
     async_call_node *prsp=(async_call_node *)arg;
     prsp->pmanager->do_sync_response(error_code,prsp->pkg_id);
     delete prsp; prsp=NULL;
   }

   int remote_sync_manager::get_sended_size()
   {
       CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_SENEDMAP),false);
       return m_sended_map.size();
   }


   void remote_sync_manager::do_sync_response(int error_code, int pkg_id)
   {
     request_sync* pNode= NULL;
     {
       CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_SENEDMAP),true);
       CDupNodePkgMapIter itr= m_sended_map.find(pkg_id);
       if(itr==m_sended_map.end())
       {
         //get index but not exist.
         log_warn("get response %d,but not found in sened map",pkg_id);
         return;
       }
       //yes. wee get it.
       pNode= itr->second; 
       m_sended_map.erase(itr);
     }

     if(0==error_code || TAIR_RETURN_DATA_NOT_EXIST==error_code)
     {
       if(pNode) {delete pNode; pNode=NULL;}
     }
     else 
     {
       //TAIR_RETURN_TIMEOUT
       if(++pNode->failed<3)
       {
         m_slow_queue.put(pNode);
       }
       else
       {
         log_error("response:pkg %d response with %d.failed=%d,log it",pkg_id,error_code,pNode->failed);
         m_sync_log_file.append(pNode);
         delete pNode; pNode=NULL;
       }
     }
     return ;
   }


   void remote_sync_manager::run(tbsys::CThread *thread, void *arg)
   {
      UNUSED(thread);
      while (!_stop) 
	  {
		try 
		{   
          int index=(long)arg;
          switch (index)
          {
            case 0:
              handle_tosend_queue();
              break;
            case 1:
              handle_slow_queue();
              break;
            case 2:
              handle_monitor_init();
              break;
            default:
              break;
          }
		}   
		catch(...)
		{   
          log_warn("unknow error! get timeoutqueue error!");
		} 
	  }
   }

    void remote_sync_manager::handle_monitor_init()
    {
      //check file has changed.
      if(!m_inited && !init())
      {
        log_error("monitor reinit failed");
      }
      else
      {
        //check the conf file has changed.
      }
      sleep(60);
    }

    void remote_sync_manager::handle_tosend_queue()
    {
        //check if the sended map was overload.
        //
        if(get_sended_size()>MAX_SYNC_SENDED_MAP_SIZE)
        {
          sleep(1);
          return;
        }
        
        //get _pkg from tosend queue or get from disk.
        request_sync * _pkg=m_tosend_queue.get(1000);
        if(!_pkg) return;

        int rv=do_send_packet(_pkg);
        //should never happen,because async client always return 0;
        if(0!=rv)
        {
          //remove from map first.
          if(m_slow_queue.size()<MAX_SLOW_LIST_SIZE)
          {
            log_warn("never:quick send failed packet area=%d with %d,slow it",_pkg->area,rv);
            m_slow_queue.put(_pkg);
          }
          else
          {
            log_error("never:quick send failed packet area=%d with %d,drop it",_pkg->area,rv);
            delete _pkg;_pkg=NULL;
          }
        }
    }

    void remote_sync_manager::handle_slow_queue()
    {
      //get _pkg from tosend queue or get from disk.
      request_sync * _pkg=m_slow_queue.get(2000);
      if(!_pkg) return;


      int rv=0;
      while(_pkg->failed<3)
      {
        sleep((_pkg->failed+1));
        rv=do_send_packet(_pkg);
        if(0!=rv)
        {
          //never go here.async send allways success.
          _pkg->failed++;
        } 
        else
        {
          break;
        }
      }

      if(rv!=0)
      {
        log_error("slow:failed resend area=%d,code=%d,log it.",_pkg->area,rv);
        //now will just log the node.
        m_sync_log_file.append(_pkg);
        delete _pkg;_pkg=NULL;
      }
    }

    int remote_sync_manager::do_send_packet(request_sync * pkg)
    {
      async_call_node *pnode=NULL;

      //reget tairmanager so we know what to do.
      int rv=0;
      if(tair_mgr->is_local(pkg->key))
      {
        rv=tair_mgr->get(pkg->area, pkg->key,pkg->value);
      }
      else
      {
        log_debug("migrated key,use local client get it");
        if(!m_local_tairclient.isinited()&& !init_local_client())
        {
          log_error("get key with local client failed with %d",TAIR_RETURN_REMOTE_NOLOCAL);
          return  TAIR_RETURN_REMOTE_NOLOCAL;
        }
        else
        {
          data_entry *_value= NULL;
          rv=m_local_tairclient.get(pkg->area, pkg->key,_value);
          if(TAIR_RETURN_SUCCESS==rv)
          {
            pkg->value=*_value;
            delete _value; _value=NULL;
          }
        }
      }
      
      if(TAIR_RETURN_SUCCESS!=rv&&TAIR_RETURN_DATA_NOT_EXIST!=rv) 
      {
        log_error("reget packet area=%d failed,rv=%d.",pkg->area,rv);
        return rv;
      } 

      int _action=(TAIR_RETURN_SUCCESS==rv)?TAIR_REQ_PUT_PACKET:TAIR_REQ_REMOVE_PACKET;

      uint32_t packet_id=atomic_add_return(1,&packet_id_creater);
      //keep it first.
      {
        CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_SENEDMAP),true);
        m_sended_map[packet_id]=pkg;
      }

      switch(_action)
      {
        case TAIR_REQ_PUT_PACKET: 
          pnode=new async_call_node(packet_id,this);
          log_debug("call tairclient put,pkgid=%d",packet_id);
          rv=m_remote_tairclient.put(pkg->area, pkg->key, pkg->value,0,0,&callback_sync_response,(void *)pnode); 
          break;
        case TAIR_REQ_REMOVE_PACKET: 
          pnode=new async_call_node(packet_id,this);
          rv=m_remote_tairclient.remove(pkg->area, pkg->key,&callback_sync_response,(void *)pnode); 
          break;
        default:
          log_error("unhandle action,pkgidr=%d,action=%d",packet_id,pkg->action);
          break;
      }
      if(0!=rv)
      {
        {
          CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_SENEDMAP),true);
          CDupNodePkgMapIter itr= m_sended_map.find(packet_id);
          if(itr!=m_sended_map.end())
          {
            m_sended_map.erase(itr);
          }
        }
        log_error("never:tairclient call failed ,pkgid=%d,rv=%d",packet_id,rv);
        delete  pnode;
      }
      return rv;
    }
}

