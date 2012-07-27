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


namespace tair{

   enum {
      REMOTE_LOCK_INT = 0,
      REMOTE_LOCK_MAX = 1,
   };

   int remote_sync_manager::get_hook_point() {
     return plugin::HOOK_POINT_RESPONSE;
   }

   int remote_sync_manager::get_property() {
     return 1003;
   }
   int remote_sync_manager::get_plugin_type() {
     return plugin::PLUGIN_TYPE_SYSTEM;
   }

   void remote_sync_manager::do_response(int rc,int call_type,int area,const data_entry* key,const data_entry* value, void* exv)
   {
     if (TAIR_RETURN_SUCCESS!=rc && TAIR_DUP_WAIT_RSP!=rc) return;
     if(!m_inited)
     {
       log_error("remote sync enabled but not inited.");// TAIR_RETURN_REMOTE_NOTINITED
       return;
     }

     switch (call_type)
     {
       case TAIR_REQ_PUT_PACKET:
       case TAIR_REQ_REMOVE_PACKET:
       case TAIR_REQ_INCDEC_PACKET:
       case TAIR_REQ_LOCK_PACKET:
         {
           data_entry * pnode= new  data_entry();
           pnode->clone(*key);
           m_tosend_queue.put(pnode);
         }
         break;
       default:
         break;
     }
     return;
   }

   void remote_sync_manager::clean()
   {
     log_debug("remote_sync_manager::clean()");
     return;
   }
   extern "C" remote_sync_manager* create()
   {
     return new remote_sync_manager();
   }

   extern "C" void destroy (remote_sync_manager* p)
   {
     delete p;
   }

   remote_sync_manager::remote_sync_manager()
   {
     atomic_set(&packet_id_creater, 0);
     m_slots_locks =new  CSlotLocks(REMOTE_LOCK_MAX);
     m_inited=false;
   }

   bool remote_sync_manager::init()
   {
     log_error("sync plugin can't init without para");
     return false;
   }

   bool remote_sync_manager::parse_para(const char* para,struct sync_conf &_remote,struct sync_conf &_local)
   {
     char _cfg[2][NAME_MAX];
     int _num=sscanf(para,"{remote:{%[^}]},local:{%[^}]}}",_cfg[0],_cfg[1]);
     if(2!=_num) return false;
     return _remote.split(_cfg[0]) && _local.split(_cfg[1]);
   }

   bool remote_sync_manager::init(const std::string& para)
   {
     //case reinit;
     if(m_inited) return m_inited;
     CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_INT),true);
     if(m_inited) return m_inited;

     char *base_home=getenv("TAIR_HOME");
     if(base_home)
       strncpy(m_base_home,base_home,NAME_MAX);  //base_home never null
     else
     {
#ifdef TAIR_DEBUG
       strcpy(m_base_home,"./");
#else
       strncpy(m_base_home,"/home/admin/tair_bin/",NAME_MAX);  //base_home never null
#endif
     }

     //{remote:{10.232.4.25,10.232.4.26,group_dup,data/sync},local:{10.232.4.25,10.232.4.26,group_1,nop}}
     //split paras 
     if(para.size()<=0 || !parse_para(para.c_str(),_remote_conf,_local_conf))
     {
       log_error("parse sync config error,para=%s",para.c_str());
       return false;
     }
     log_info("remote plugin start init with para=%s",para.c_str());

     if(strlen(_remote_conf.remote_data_dir)>0)
     {
       memset(m_data_dir,0,NAME_MAX);
       sprintf(m_data_dir,"%s/%s",m_base_home,_remote_conf.remote_data_dir); //keep for rotate
       if(!tbsys::CFileUtil::mkdirs(m_data_dir)) 
       {
         log_error("sync config mkdir datadir %s error", _remote_conf.remote_data_dir);
         return false;
       }

       char binlog[NAME_MAX];
        snprintf(binlog,255,"%s/%s.%d",m_data_dir,LOG_FILE_NAME,getpid());
         if(!m_tosend_queue.enable_disk_save(binlog,false))
         {
           log_error("open data file failed %s",m_data_dir);
           return false;
         }

       //set failed log key file.
       snprintf(binlog,255,"%s/binlog.%d",m_data_dir,getpid());
       if(!m_sync_log_file.start(binlog,false))
       {
         log_error("open binlog file %s falied ",binlog);
         return false;
       }
     }

     //check para.
     if(strlen(_remote_conf.master_ip)<=0 || strlen(_remote_conf.group_name)<=0 || 
         strlen(_local_conf.master_ip)<=0 || strlen(_local_conf.group_name)<=0)
     {
       log_error("sync config not master or group");
       return false;
     }

     m_remote_tairclient.set_timeout(2000);
     if(!m_remote_tairclient.startup(_remote_conf.master_ip,_remote_conf.slave_ip,_remote_conf.group_name))
     {
       log_warn("remote:%s,%s can not connect,wait reconnect",_remote_conf.master_ip,_remote_conf.group_name);
     }
     setThreadCount(MAX_DUP_THREAD+1);
     this->start();
     m_inited=true;

     log_info("remote plugin finish init with para=%s",para.c_str());
     return true;
   }

   bool remote_sync_manager::init_tair_client(tair_client_impl&  _client,struct sync_conf &_conf)
   {
     CScopedRwLock __scoped_lock(m_slots_locks->getlock(REMOTE_LOCK_INT),true);
     if(_client.isinited()) return true;
     if(!_client.startup(_conf.master_ip,_conf.slave_ip,_conf.group_name))
     {
       log_error("remote:%s,%s can not connect ",_conf.master_ip,_conf.group_name);
       return false;
     }
     return true;
   }

   remote_sync_manager::~remote_sync_manager()
   {
      this->stop();
      this->wait();
      delete  m_slots_locks;
   }

   void remote_sync_manager::callback_sync_response(int error_code, void * arg)
   {
     //tairclient start single thread do it.
     async_call_node *prsp=(async_call_node *)arg;
     prsp->pmanager->do_sync_response(error_code,prsp);
     delete prsp; prsp=NULL;
   }

   void remote_sync_manager::do_sync_response(int error_code,async_call_node *prsp)
   {
     if(0!=error_code && TAIR_RETURN_DATA_NOT_EXIST!=error_code)
     {
       //TAIR_RETURN_TIMEOUT,retry it.
       if(++prsp->failed<=2)
       {
         m_slow_queue.put(prsp->pkey);
       }
       else
       {
         log_error("response:pkg %d response with %d.failed=%d,log it",prsp->pkg_id,error_code,prsp->failed);
         do_invalid_key(prsp->pkey);
       }
     }else
     {
       delete prsp->pkey;
     }

     prsp->pkey=NULL;
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
             handle_slow_queue();
             break;
           case 1:
           default:
             handle_tosend_queue(index);
             break;
         }
       }   
       catch(...)
       {   
         log_warn("unknow error! get timeoutqueue error!");
       } 
     }
   }

    void remote_sync_manager::handle_tosend_queue(uint16_t index)
    {
      //check if the sended map was overload.

      //get _pkg from tosend queue or get from disk.
      data_entry * _pkg=m_tosend_queue.get(1000);
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
          //todo. do invalid
        }
      }
    }

    void remote_sync_manager::handle_slow_queue()
    {
      //get _pkg from tosend queue or get from disk.
      data_entry * _pkg=m_slow_queue.get(2000);
      if(!_pkg) return;

      int failed=0;
      int rv=0;
      while(failed++<3)
      {
        sleep(failed);
        rv=do_send_packet(_pkg);
        if(0!=rv)
        {
          //never go here.async send allways success.
          failed++;
        } 
        else
        {
          break;
        }
      }

      if(rv!=0)
      {
        //invalid and log it.
        log_error("slow:failed resend area=%d,code=%d,invalid and log it.",_pkg->area,rv);
        do_invalid_key(_pkg);
      }
    }

    int remote_sync_manager::do_direct_send_packet(int call_type,int area,const data_entry* key,const data_entry* value)
    {
      int rv=TAIR_RETURN_REMOTE_NOLOCAL;

      if(!m_remote_tairclient.isinited()&& !init_tair_client(m_remote_tairclient,_remote_conf))
      {   
        return  TAIR_RETURN_REMOTE_NOLOCAL;
      }
      if(TAIR_REQ_PUT_PACKET!=call_type || TAIR_REQ_REMOVE_PACKET!=call_type) return rv;

      async_call_node *pnode=NULL;
      uint32_t packet_id=atomic_add_return(1,&packet_id_creater);

      data_entry * pkey= new  data_entry();
      pkey->clone(*key);

      switch(call_type)
      {
        case TAIR_REQ_PUT_PACKET: 
          {
            pnode=new async_call_node(packet_id,pkey,this);
            log_debug("call tairclient put,pkgid=%d",packet_id);
            rv=m_remote_tairclient.put(area, *pkey, *value,0,0,false,&callback_sync_response,(void *)pnode); 
          }
          break;
        case TAIR_REQ_REMOVE_PACKET: 
          pnode=new async_call_node(packet_id,pkey,this);
          rv=m_remote_tairclient.remove(area, *pkey,&callback_sync_response,(void *)pnode); 
          break;
        default:
          break;
      }
      return rv;
    }

    int remote_sync_manager::do_send_packet(data_entry * pkey)
    {
      async_call_node *pnode=NULL;

      //reget key so we know what to do.
      data_entry *pvalue= NULL;

      if(!m_local_tairclient.isinited()&& !init_tair_client(m_local_tairclient,_local_conf))
      {   
        log_error("get key with local client failed with %d",TAIR_RETURN_REMOTE_NOLOCAL);
        return  TAIR_RETURN_REMOTE_NOLOCAL;
      }   

      if(!m_remote_tairclient.isinited()&& !init_tair_client(m_remote_tairclient,_remote_conf))
      {   
        return  TAIR_RETURN_REMOTE_NOLOCAL;
      }

      int rv=m_local_tairclient.get(pkey->area, *pkey,pvalue);
      if(TAIR_RETURN_SUCCESS!=rv&&TAIR_RETURN_DATA_NOT_EXIST!=rv) 
      {
        log_error("reget packet area=%d failed,rv=%d.",pkey->area,rv);
        return rv;
      } 

      int _action=(TAIR_RETURN_SUCCESS==rv)?TAIR_REQ_PUT_PACKET:TAIR_REQ_REMOVE_PACKET;
      uint32_t packet_id=atomic_add_return(1,&packet_id_creater);

      switch(_action)
      {
        case TAIR_REQ_PUT_PACKET: 
          //check verson.
          {
            pnode=new async_call_node(packet_id,pkey,this);
            log_debug("call tairclient put,pkgid=%d",packet_id);
            rv=m_remote_tairclient.put(pkey->area, *pkey, *pvalue,0,0, false, &callback_sync_response,(void *)pnode); 
            delete pvalue;
          }
          break;
        case TAIR_REQ_REMOVE_PACKET: 
          pnode=new async_call_node(packet_id,pkey,this);
          rv=m_remote_tairclient.remove(pkey->area, *pkey,&callback_sync_response,(void *)pnode); 
          break;
        default:
          log_error("unhandle action,pkgidr=%d,action=%d,not happened",packet_id,_action);
          break;
      }
      return rv;
    }

    int remote_sync_manager::do_invalid_key(data_entry *& pkey)
    {
      int rv=m_remote_tairclient.invalidate(pkey->area,*pkey);
      if(0!=rv)
      {
        log_error("invalid key error with rv=%d,area=%d,key=%s",rv,pkey->area,pkey->get_printable_key().c_str());
        //m_sync_log_file.append(pkey);
      }
      delete pkey;pkey=NULL;
      return rv;
    }
}

