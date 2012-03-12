#ifndef __WORKER__
#define __WORKER__

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <iostream>
#include <vector>

#include <tbsys.h>
#include <tbnet.h>

#include "nonblock_queue.hpp"
#include "del_job.hpp"
#include "tair_client_api_impl.hpp"

#include "define.hpp"
#include "item_manager.hpp"
#include "util.hpp"
#include "dump_data_info.hpp"
#include "query_info_packet.hpp"

namespace tair {

#define _WORKER_WAIT_TIME    1000
#define _MAX_QUEUE_SIZE 100
#define _TRY_SEND_TIEMS 3

enum {
    NO_FORMAT=0,
    JAVA_FORMAT=1,
    RAW_FORMAT=2
};

class CleanWorker {
public:
    CleanWorker():_queue(_MAX_QUEUE_SIZE){}
    CleanWorker(const char* server_addr, const char* group_name, const int area,
            const int key_format, const char* name = NULL);
    ~CleanWorker();
    void init(const char* server_addr, const char* group_name, const int area,
            const int key_format, const char* name = NULL);
    void start();
    void run();
    void stop();
    int offerJob(DelJob* job);
    char* getName();
private:
    static void* thread_func(void* argv);
    char* canonical_key(char *key, int *size);
    void doDelete(char** keys, int keynum);
    NBQueue<DelJob*> _queue;
    pthread_t _work_thread;
    tair_client_impl client_helper;
    int _can_over;
    char* _name;

    char* _server_addr;
    char* _group_name;
    int _area;
    int _key_format;
};

CleanWorker::CleanWorker(const char* server_addr, const char* group_name, const int area,
        const int key_format, const char* name)
    : _queue(_MAX_QUEUE_SIZE){
    init(server_addr, group_name, area, key_format, name);
}

CleanWorker::~CleanWorker() {
    _can_over = 1;
    pthread_join(_work_thread, NULL);
    if (_name != NULL) {
        delete _name;
    }

    if (_server_addr != NULL) {
        delete []_server_addr;
    }

    if (_group_name != NULL) {
        delete []_group_name;
    }
}

void CleanWorker::init(const char* server_addr, const char* group_name, const int area,
        const int key_format, const char* name) {
    _server_addr = NULL;
    _group_name = NULL;

    if (name == NULL) {
        _name = NULL;
    } else {
        _name = new char[strlen(name) + 1];
        strcpy(_name, name);
    }

    _can_over = 0;
    
    int len_server_addr = strlen(server_addr) + 1;
    _server_addr = new char[len_server_addr];
    if (_server_addr == NULL) {
        fprintf(stderr, "file %s, line %d: no enough memory\n",
                __FILE__, __LINE__);
        exit(1);
    }
    strcpy(_server_addr, server_addr);
    
    int len_group_name = strlen(group_name) + 1;
    _group_name = new char[len_group_name];
    if (_group_name == NULL) {
        fprintf(stderr, "file %s, line %d: no enough memory\n",
                __FILE__, __LINE__);
        exit(1);
    }
    strcpy(_group_name, group_name);
    
    _area = area;
    _key_format = key_format;
}

void CleanWorker::start() {
    client_helper.set_timeout(5000);
    bool done = client_helper.startup(_server_addr, NULL, _group_name);
    if (done == false) {
        fprintf(stderr, "file %s, line %d: %s cann't connect.\n",
                __FILE__, __LINE__, _server_addr);
        return;
    }
    int flag = pthread_create(&_work_thread, NULL, thread_func, this);
    if (flag < 0) {
        fprintf(stderr, "file %s, line %d: %s\n",
                __FILE__, __LINE__, strerror(errno));
        return;
    }
}

void* CleanWorker::thread_func(void* argv) {
    CleanWorker* worker = (CleanWorker*)argv;
    worker->run();
    return NULL;
}

void CleanWorker::run() {
    while(true) {
        if(_queue.isEmpty()) {
            if (_can_over) {
                break;
            }
            usleep(_WORKER_WAIT_TIME);
            continue;
        }
        DelJob* deljob = _queue.pop();
        doDelete(deljob->getAllKey(), deljob->getSize());
        delete deljob;
    }
}

int CleanWorker::offerJob(DelJob* job) {
    if (_queue.isFull()) {
        return 0;
    }
    _queue.push(job);
    return 1;
}

char* CleanWorker::getName() {
    return _name;
}

void CleanWorker::doDelete(char** keys, int keynum) {
    std::vector<data_entry*> keys_v;
    for (int i = 0; i < keynum; ++i)
    {
      int pkeysize = 0;
      char *pkey = canonical_key(keys[i], &pkeysize);
      data_entry* key = new data_entry(pkey, pkeysize, false);
      keys_v.push_back(key);
    }
   
    for(int i = 0; i < keynum; ++i) {
        int ret, j;
        for(j = 0; j < _TRY_SEND_TIEMS; ++j) {
            ret = client_helper.remove(_area, *(keys_v[i]));
            if (ret == TAIR_RETURN_SUCCESS || TAIR_RETURN_DATA_NOT_EXIST) {
                break;
            }
        }
        if (j >= _TRY_SEND_TIEMS) {
            fprintf(stderr, "DELETE: %s, ret: %d\n", client_helper.get_error_msg(ret), ret);
        }
    }

    std::vector<data_entry*>::iterator vit = keys_v.begin();
    for ( ; vit != keys_v.end(); ++vit)
    {
        if (_key_format != NO_FORMAT) {
            char* data = (*vit)->get_data();
            free(data);
        }
        delete *vit;
        (*vit) = NULL;
    }
}

char* CleanWorker::canonical_key(char *key, int *size)
{
   char *pdata = key;
   if (_key_format == JAVA_FORMAT) { // java format
      *size = strlen(key)+2;
      pdata = (char*)malloc(*size);
      pdata[0] = '\0';
      pdata[1] = '\4';
      memcpy(pdata+2, key, strlen(key));
   } else if (_key_format == RAW_FORMAT) { // raw format
      pdata = (char*)malloc(strlen(key)+1);
      util::string_util::conv_raw_string(key, pdata, size);
   } else if (_key_format == NO_FORMAT) { // no format
      *size = strlen(key) + 1;
   }

   return pdata;
}

}

#endif
