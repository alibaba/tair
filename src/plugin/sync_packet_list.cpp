/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 */

#include "sync_packet_list.hpp"

namespace tair{
using namespace tair::common;

enum {
  SYNC_LOCK_MEM= 0,
  SYNC_LOCK_DISK=1 ,
  SYNC_LOCK_MAX=2 ,
};

sync_packet_list::sync_packet_list()
{
  m_slots_locks = new  common::CSlotLocks(SYNC_LOCK_MAX);
  atomic_set(&m_disk_stored, STATE_MEM_STORE);
  m_use_disk=false;
  m_pout_queue=&m_in_queue;
}

sync_packet_list::~sync_packet_list()
{
  delete  m_slots_locks;
}

unsigned long sync_packet_list::size()
{
  //only care memory size.
  return m_in_queue.size();
}

bool sync_packet_list::enable_disk_save(const std::string& disk_save_name,bool replay)
{
  m_disk_path=disk_save_name;
  m_use_disk=true;
  //open file with the only file.
  if(!m_disklist_file.start(m_disk_path,replay))
  {
    log_error("open disk file failed with %s",m_disk_path.c_str());
    return false;
  }
  //now i will check file count,if it's a smaller than read it into memory entirely.
  int _size=m_disklist_file.get_count();
  if(_size>0)
  {
    log_info("load %d record from file %s",_size,m_disk_path.c_str());
    atomic_set(&m_disk_stored, STATE_DISK_STORE);
  }
  return true;
}

int sync_packet_list::do_batch_load(unsigned int _want_num)
{
  //CScopedRwLock __scoped_lock(m_slots_locks->getlock(SYNC_LOCK_DISK),true);
  std::vector<data_entry *> _vector_node;
  bool bneed_switch=!m_disklist_file.batch_get(_want_num,_vector_node);

  if(bneed_switch||_vector_node.size()<_want_num)
  {
    atomic_set(&m_disk_stored, STATE_MEM_STORE);
    //it's lock free,so we should reget it to fetch the new one.
    bneed_switch=m_disklist_file.batch_get(_want_num-_vector_node.size(),_vector_node);
    int _store_stated= atomic_read(&m_disk_stored);
    if(STATE_MEM_STORE==_store_stated && bneed_switch)
    {
      log_info("load %lu from file finished,switch to memory",_vector_node.size());
      m_disklist_file.clear();
    }
  }

  log_info("load %lu from file ,want %d",_vector_node.size(),_want_num);
  for(unsigned int i=0;i<_vector_node.size();i++)
  {
    //m_pout_queue dones't care sequece.
    m_pout_queue->put(_vector_node[i]);
  }
  return _vector_node.size();
}


int sync_packet_list::put(data_entry *& value)
{
  //CScopedRwLock __scoped_lock(m_slots_locks->getlock(SYNC_LOCK_MEM),true);
  int _store_stated= atomic_read(&m_disk_stored);
  if(STATE_MEM_STORE==_store_stated)
  {
    if(m_in_queue.size()<MAX_SYNC_LIST_SIZE)
    {
      m_in_queue.put(value);
      return 0;
    }
    else if(m_use_disk)
    {
      log_info("queue size exceed %lu,switch to disk save", m_in_queue.size());
      atomic_set(&m_disk_stored, STATE_DISK_STORE);
    }
  }
  //reget it
  _store_stated= atomic_read(&m_disk_stored);
  if(STATE_DISK_STORE==_store_stated)
  {
    //just push it into disk list and delete the pointer.
    CScopedRwLock __scoped_lock(m_slots_locks->getlock(SYNC_LOCK_DISK),true);
    m_disklist_file.append(value);
    //now i will delete the value.
    delete value; value=NULL;
    return 0;
  }
  return TAIR_RETURN_REMOTE_SLOW;
}

int sync_packet_list::put_head(data_entry *& value)
{
  //i will put to head if necessary.
  return this->put(value);
}

data_entry * sync_packet_list::get(int tmms)
{
  data_entry  *pnode=m_pout_queue->get(tmms);
  if(pnode) return pnode;

  int _store_stated= atomic_read(&m_disk_stored);

  //get null from queue. so read from  disk.
  if(STATE_DISK_STORE==_store_stated)
  {
    CScopedRwLock __scoped_lock(m_slots_locks->getlock(SYNC_LOCK_DISK),true);
    //batch load  packet to m_pout_queue;
    if(m_pout_queue->size()<=0) do_batch_load(MAX_SYNC_LIST_SIZE);
    return m_pout_queue->get(tmms);
  }
  return NULL; //just return and wait for next loop.
}

}
