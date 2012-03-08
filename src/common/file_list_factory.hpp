/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * 
 * save or load rec from seq file.
 *
 *
 */

#ifndef TAIR_FILE_LIST_FACTORY_H
#define TAIR_FILE_LIST_FACTORY_H

#include <stdint.h>
#include <ext/hash_map>

#include <tbsys.h>
#include <tbnet.h>
#include "file_op.hpp" 
#include "define.hpp" 
#include "log.hpp"

namespace tair{
  namespace common{

#define LOG_RECORD_HEAD_SIZE 0
#define LOG_RECORD_SIZE 1024

  template <class T>
  class RecordLogFile
  {
    public:
      RecordLogFile();
      ~RecordLogFile();
    public:
      bool start(const std::string& file_name,bool try_replay=true);
      bool check_init();
      bool clear();
    public:
      int  append(T*  value);
      bool get(T& value);
      bool get(unsigned int index,T& value);
      bool batch_get(unsigned int max_count,std::vector<T *>& _item_vector);
      uint64_t get_count();
    private:
      tair::common::FileOperation* file_;
      int64_t file_offset_;
      int last_read_index;
  };

  template <class T>
  RecordLogFile<T>::RecordLogFile():file_(NULL),file_offset_(0),last_read_index(0)
  {
  }

  template <class T>
  bool RecordLogFile<T>::start(const std::string& name,bool try_replay)
  {
    if(name.size()<=0) return false;
    if (file_ !=NULL)
    {
      log_warn("RecordLogFile already init, filename: %s", file_->get_file_name());
      return true;
    }
    bool need_new_log = !try_replay || ::access(name.c_str(), F_OK) != 0;
    file_ = new tair::common::FileOperation(name);
    if (file_->open_file() < 0)
    {
      log_error("open log file %s fail, error: %s", name.c_str(), strerror(errno));
      return false;
    }
    log_debug("open log file %s", name.c_str());
    //check file size and rescure the log file.
    if(!need_new_log)
    {
      file_offset_=get_count()*LOG_RECORD_SIZE;
    }else{
			if(0!=file_->ftruncate_file(0))
			{
					log_error("ftruncate_file file %s failed.", name.c_str());
          return false;
			}
		}

    return true;
  }

  template <class T>
  RecordLogFile<T>::~RecordLogFile()
  {
    if (!file_ ) delete file_;
  }

  template <class T>
  bool RecordLogFile<T>::check_init()
  {
    return file_ != NULL;
  }

  template <class T>
  int RecordLogFile<T>::append(T* value)
  {
    if (!check_init()) return TAIR_RETURN_REMOTE_NOTINIT;

    tbnet::DataBuffer _output;
    _output.ensureFree(LOG_RECORD_SIZE);

    value->encode(&_output); 

    int32_t write_len = file_->pwrite_file(_output.getData(),LOG_RECORD_SIZE, file_offset_);
    if (write_len != LOG_RECORD_SIZE)
    {
      log_error("add record fail, write fail. ret: %d", write_len);
      return TAIR_RETURN_REMOTE_DISKSAVE_FAILED;
    }
    else
    {
      file_offset_ += LOG_RECORD_SIZE;
    }
    return 0;
  }

  template <class T>
  bool RecordLogFile<T>::get(T& value)
  {
    return get(last_read_index++,value);
  }

  //if(!pnew_node) pnew_node= new request_sync();
  template <class T>
  bool RecordLogFile<T>::batch_get(unsigned int max_count,std::vector<T *>& _item_vector)
  {
    if(!check_init()) return false;
    int64_t offset=LOG_RECORD_SIZE*last_read_index;
    char * puffer= new char[LOG_RECORD_SIZE*max_count];


    int32_t read_len=file_->pread_file(puffer,LOG_RECORD_SIZE * max_count, offset);
    if(read_len<LOG_RECORD_SIZE)
    {
      log_error("read logfile faild. read_len: %d", read_len);
      return false;
    }
    int _count=read_len/LOG_RECORD_SIZE;
    last_read_index+=_count;

    for(int i=0;i<_count;i++)
    {
      tbnet::DataBuffer _input;
      _input.ensureFree(LOG_RECORD_SIZE);
      memcpy(_input.getFree(),puffer+i*LOG_RECORD_SIZE,_input.getFreeLen());
      _input.pourData(LOG_RECORD_SIZE);
      T *value  =new T;
      if(value->decode(&_input))
      {
        _item_vector.push_back(value);
      }
      else
      {
        delete value; value=NULL;
      }
    }
    return true;
  }

  template <class T>
  bool RecordLogFile<T>::get(unsigned int index,T& value)
  {
    if(!check_init()) return false;

    uint32_t record_count = get_count();
    if (record_count <=0 || record_count<index)
    {
      log_error("get index out of range,file %s is invalid", file_->get_file_name());
      return false;
    }
    //read one.
    tbnet::DataBuffer _input;
    _input.ensureFree(LOG_RECORD_SIZE);
    int32_t read_len = 0;
    int64_t offset=  LOG_RECORD_SIZE* index;
    if ((read_len = file_->pread_file(_input.getFree(), LOG_RECORD_SIZE, offset)) != LOG_RECORD_SIZE )
    {
      log_error("read logfile faild. read_len: %d", read_len);
      return false;
    }
    _input.pourData(read_len);

    value.decode(&_input);
    return true;
  }

  template <class T>
  uint64_t RecordLogFile<T>::get_count()
  {
    //must return 0 or uint64_t
    if (!check_init()) return 0;
    int32_t count = 0;
    int64_t file_size = file_->get_file_size();
    if (file_size <= 0)
    {
      log_debug("get log file size=%d, ignore",file_size);
      count = 0;
    }
    else
    {
      if ((file_size ) % LOG_RECORD_SIZE != 0)
      {
        log_error("gc log file maybe conflict. filesize: %"PRI64_PREFIX"d, replay most", file_size);
      }
      count = (file_size)/LOG_RECORD_SIZE;
    }
    log_info("record file size: %"PRI64_PREFIX"d, record count: %d,last_read_index=%d", file_size, count,last_read_index);
    return count;
  }

  template <class T>
  bool RecordLogFile<T>::clear()
  {
    log_info("now clear record file,offset=%"PRI64_PREFIX"d,read_index=%d",file_offset_,last_read_index);
    file_offset_=0;
    last_read_index=0;
    return 0==file_->ftruncate_file(0);
  }

}
}

#endif
