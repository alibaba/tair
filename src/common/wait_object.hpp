/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * wait object used for async communication
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_WAIT_OBJECT
#define TAIR_WAIT_OBJECT

#include <ext/hash_map>
#include <tbsys.h>
#include <tbnet.h>
#include "base_packet.hpp"
#include "BlockQueueEx.hpp"

using namespace std;
using namespace __gnu_cxx;

namespace tair {
   namespace common {
      typedef int tbnet_pcode_type;
      const tbnet_pcode_type null_pcode = -1;


      class wait_object {
      public:
         friend class wait_object_manager;
         wait_object()
          {
            init();
         }
         wait_object(const tbnet_pcode_type& except)
            :except_pcode(except)
         {
            init();
         }
         wait_object(const set<tbnet_pcode_type>& except_set)
            :except_pcode_set(except_set)
         {
            init();
         }

         wait_object (int _cmd,void (*_func)(int retcode, void* parg),void *parg)
         {
           init();
           m_callback_func=_func;
           //m_argnum=1;
           m_args=parg;
           m_cmd=_cmd;
         }
         ~wait_object()
         {
            tbsys::CThreadGuard guard(&mutex);
            if (resp_list != NULL) {
               for (uint32_t i=0; i<resp_list->size(); i++) {
                  if(resp_list->at(i) != NULL && resp_list->at(i)->isRegularPacket())
                     delete resp_list->at(i);
               }
               delete resp_list;
               resp_list = NULL;
            }
            if (resp != NULL && resp->isRegularPacket()) {
               delete resp;
               resp = NULL;
            }
            except_pcode_set.clear();
         }
         bool wait_done(int count, int timeout = 0)
          {
            cond.lock();
            while (done_count<count) {
               if (cond.wait(timeout) == false) {
                  cond.unlock();
                  return false;
               }
            }
            cond.unlock();
            return true;
         }
         bool insert_packet(base_packet* packet)
          {
            if (packet != NULL)
            {
               tbsys::CThreadGuard guard(&mutex);
               if (except_pcode != null_pcode && except_pcode == packet->getPCode())
                  return false;
               if (!except_pcode_set.empty() &&
                   except_pcode_set.find(packet->getPCode()) != except_pcode_set.end())
                  return false;
               if (resp_list != NULL) {
                  resp_list->push_back(packet);
               } else if (resp != NULL) {
                  resp_list = new vector<base_packet*>();
                  resp_list->push_back(resp);
                  resp = NULL;
                  resp_list->push_back(packet);
               } else {
                  resp = packet;
               }
            }
            done();
            return true;
         }
         base_packet *get_packet(int index=0)
          {
            tbsys::CThreadGuard guard(&mutex);
            if (resp_list != NULL) {
               if((size_t)index < resp_list->size())
                  return resp_list->at(index);
               return NULL;
            } else {
               return resp;
            }
         }
         int get_packet_count()
          {
            tbsys::CThreadGuard guard(&mutex);
            if (resp_list != NULL) return resp_list->size();
            if (resp != NULL) return 1;
            return 0;
         }
         int get_id() const
          {
            return id;
         }
         void set_no_free()
          {
            resp = NULL;
            resp_list = NULL;
         }
         bool is_async() { return NULL!=m_callback_func; }
         int do_async_response(int error_code)
         {
           if(m_callback_func)
           {
             m_callback_func(error_code,m_args);
           }
           return 0;
         }
          int get_cmd(){return m_cmd;}
      private:
         int id;
         void init(){
            done_count = 0;
            id = 0;
            resp = NULL;
            resp_list = NULL;
            except_pcode = null_pcode;

            m_callback_func=NULL;
            //m_argnum=0;
            m_args=NULL;
            m_cmd=0;
         }
         void done() {
            cond.lock();
            done_count ++;
            cond.signal();
            cond.unlock();
         }

         tbsys::CThreadMutex mutex;
         set<tbnet_pcode_type> except_pcode_set;
         tbnet_pcode_type except_pcode;

         base_packet *resp;
         vector<base_packet*> *resp_list;
         tbsys::CThreadCond cond;
         int done_count;
       private: //below is callback function
         void (*m_callback_func)(int retcode, void* parg);
         //unsigned short m_argnum;
         void* m_args;
         int m_cmd;
      };

#ifndef __PACKET_TIME_OUT_HINT
#define __PACKET_TIME_OUT_HINT
      class CPacket_Timeout_hint
      {
        public:
          uint32_t packet_id; 
          time_t expired;
      };
      typedef BlockQueueEx<CPacket_Timeout_hint * > CWaitPacketQueue;
#endif

      class wait_object_manager : public tbsys::Runnable{
      public:
      //@override Runnable
      void run(tbsys::CThread *thread, void *arg)
      {
        UNUSED(thread);
        UNUSED(arg);
        while (!_stop) 
        {
          try 
          {   
            CPacket_Timeout_hint * _pkg=dup_wait_queue.get(2000); //2s
            if(!_pkg) continue;
            handle_timeout_packet(_pkg);
            delete _pkg;
          }   
          catch(...)
          {   
            log_warn("unknow error! get timeoutqueue error!");
          } 
        }
      }
      public:
         wait_object_manager()
         {
           wait_object_seq_id = 1;
           handle_async_wait_object=NULL;
           m_callback_arg =NULL;
           _stop=true;
           is_async=false;
           //need't start the timeout thread.
         }

         wait_object_manager(int (*_call_func)(void* arg,wait_object *cwo),void *_caller)
         {
           wait_object_seq_id = 1;
           handle_async_wait_object=_call_func;
           m_callback_arg=_caller;
           _stop=false;
           is_async=true;
           timeout_thread.start(this, NULL);
         }

         ~wait_object_manager()
         {
           _stop=true;
           timeout_thread.join();
           tbsys::CThreadGuard guard(&mutex);
           hash_map<int, wait_object*>::iterator it;
           for (it=wait_object_map.begin(); it!=wait_object_map.end(); ++it) {
             delete it->second;
           }
         }
         wait_object* create_wait_object()
          {
            wait_object *cwo = new wait_object();
            add_new_wait_object(cwo);
            return cwo;
         }
         wait_object* create_wait_object(const tbnet_pcode_type& except)
          {
            wait_object *cwo = new wait_object(except);
            add_new_wait_object(cwo);
            return cwo;
         }
         wait_object* create_wait_object(const set<tbnet_pcode_type>& except_set)
          {
            wait_object *cwo = new wait_object(except_set);
            add_new_wait_object(cwo);
            return cwo;
         }
         wait_object* create_wait_object(int cmd,void (*_pfunc)(int retcode, void* parg),void *parg,int timeout=0)
         {
           wait_object *cwo = new wait_object(cmd,_pfunc,parg);
           add_new_wait_object(cwo);
           //log_debug("add new [%d] waitobj", cwo->id);
           //keep it in timewait.

           CPacket_Timeout_hint  *phint=new CPacket_Timeout_hint();
           phint->packet_id=cwo->id;
           phint->expired=(timeout>TAIR_MAX_CLIENT_OP_TIME||0==timeout)?TAIR_CLIENT_OP_TIME:timeout;
           phint->expired+=time(NULL);
           dup_wait_queue.put(phint);

           return cwo;
         }

         void destroy_wait_object(wait_object *cwo)
          {
            tbsys::CThreadGuard guard(&mutex);
            wait_object_map.erase(cwo->id);
            delete cwo;
         }
         void wakeup_wait_object(int id, base_packet *packet)
          {
            tbsys::CThreadGuard guard(&mutex);
            hash_map<int, wait_object*>::iterator it;
            it = wait_object_map.find(id);
            if (it != wait_object_map.end()) 
            {
              //now check the object need a async callback.so we just call the obejct's handler.
              wait_object *cwo=it->second;
              if (packet == NULL) 
              {
                log_error("[%d] packet is null.", id);
              } else if (!cwo->insert_packet(packet))
              {
                log_debug( "[pCode=%d] packet had been ignored", packet->getPCode());
                if (packet->isRegularPacket()) delete packet;
              }

              if(cwo->is_async() &&handle_async_wait_object)
              {
                wait_object_map.erase(cwo->id);
                log_debug("async wakeup object packet,id=%d\n",id);
                handle_async_wait_object(m_callback_arg,cwo);
                return;
              }
              log_debug("sync wakeup object packet,id=%d\n",id);
            } else {
               if (packet != NULL && packet->isRegularPacket()) delete packet;
               log_debug("[%d] waitobj not found.", id);
            }
         }

      private:
         void add_new_wait_object(wait_object *cwo)
          {
            tbsys::CThreadGuard guard(&mutex);
            wait_object_seq_id ++;
            if (wait_object_seq_id == 0) wait_object_seq_id ++;
            cwo->id = wait_object_seq_id;;
            wait_object_map[cwo->id] = cwo;
         }
        void handle_timeout_packet(CPacket_Timeout_hint  * phint)
        {
          time_t _now=time(NULL);
          int _left=phint->expired-_now;
          if(_left>0)
          {
            sleep(_left>TAIR_SERVER_OP_TIME?TAIR_SERVER_OP_TIME:_left);
          }
          //no it's timeout.
          wakeup_wait_object(phint->packet_id,NULL);
          usleep(300);
        }
      private:
         hash_map<int, wait_object*> wait_object_map;
         int wait_object_seq_id;
         tbsys::CThreadMutex mutex;
      private:
         //atomic_t packet_id_creater;
         bool _stop;
         bool is_async;
         tbsys::CThread timeout_thread;
	       CWaitPacketQueue dup_wait_queue;
         int (*handle_async_wait_object)(void * phandler,wait_object * obj);
         void *m_callback_arg ;
      };
   }

}

#endif
