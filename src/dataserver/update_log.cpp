/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * update log is for record updates while migrating
 *
 * Version: $Id$
 *
 * Authors:
 *   fanggang <fanggang@taobao.com>
 *     - initial release
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <sstream>
#include <iostream>
#include "log.hpp"
#include "update_log.hpp"

namespace tair {

   using namespace std;
   using namespace tair::storage::fdb;
   using namespace __gnu_cxx;

   update_log* update_log::open(const char *dir, const char *name, uint32_t log_file_number, bool is_migrating, uint32_t file_size)
   {
      update_log* log = new update_log(is_migrating);
      uint32_t log_file_num = update_log::scan_log_directory(dir, name);
      log->current_log_file_number = log_file_num;
      log->log_file_number = log_file_number;
      log->base_name = new char[strlen(dir) + strlen(name) + 2];
      strcpy(log->base_name, dir);
      strcat(log->base_name, "/");
      strcat(log->base_name, name);
      log->file_size = file_size;
      log->ctr_file = control_file::open(dir);
      log->start_up(is_migrating);
      return log;
   }

   void update_log::init()
   {
      file_manager = NULL;
      reader = NULL;
      writer = NULL;
   }

   update_log::update_log(bool is_migrating)
   {
      this->is_migrating = is_migrating;
      init();
   }

   update_log::~update_log() 
   {

   }

   lsn_type update_log::get_current_lsn()
   {
      return writer->get_next_write_lsn();
   }

   int update_log::filter(const struct dirent* dirp, const char *name)
   {
      if(!strncmp(dirp->d_name, name, strlen(name))) {
         return 1;
      } else {
         return 0;
      }
   }

   uint32_t update_log::scan_log_directory(const char *dir, const char *name)
   {
      bool is_dir = tbsys::CFileUtil::isDirectory(dir);
      if (!is_dir) {
         bool ret = tbsys::CFileUtil::mkdirs((char *)dir);
         if (!ret){
            log_error("make update log directory failed: %s\n", dir);
         }
         return 0;
      }
      struct dirent **name_list;
      int n;
      n = scandir(dir, &name_list, 0, NULL);
      if (n < 0) {
         log_error("scan log dir error");
         return 0;
      } else {
         int tmp =n;
         while(n--) {
            if(!update_log::filter(name_list[n], name)) {
               tmp--;
            }
            free(name_list[n]);
         }
         free(name_list);
         return tmp;
      }
   }

   void update_log::start_up(bool is_migrating)
   {
      file_manager = log_file_manager::open(this, base_name, log_file_number, current_log_file_number, file_size);
      log_read_buffer *log_read_buff = new log_read_buffer(file_manager);
      reader = new log_reader(log_read_buff);
      if(is_migrating) {
         file_manager->reset();
         ctr_file->reset();
         writer = new log_writer(this, MIN_LSN);
         return;
      }
      lsn_type tail_lsn = find_log_tail();
      if (tail_lsn == INVALID_LSN){
         tail_lsn = file_manager->init_on_first_open();
      } else {
         if (file_manager->find_log_file(tail_lsn)) {
            file_manager->set_current_log_file(tail_lsn);
         } else {
            file_manager->set_current_log_file(tail_lsn - 1);
         }
      }

      tail_lsn = remove_incomplete_log_entry(tail_lsn);

      writer = new log_writer(this, tail_lsn);

      lsn_type last_lsn = get_hlsn();
      lsn_type real_lsn = tail_lsn > last_lsn ? last_lsn : tail_lsn;
      real_lsn = 0;
   }

   void update_log::log(sn_operation_type operation_type, data_entry &key, data_entry &value, uint16_t db_id)
   {
      uint key_size = key.get_size();
      if (key.data_meta.keysize == 0){
         key.data_meta.keysize = key_size;
      }

      log_debug("key length: %d, value length:%d", key_size, value.get_size());
      uint value_size = 0;
      uint header_size = sizeof(item_meta_info);
      int total_size = header_size + key_size + 3 ;
      if (operation_type == SN_PUT) {
         value_size = value.get_size();
         if (key.data_meta.valsize == 0){
            key.data_meta.valsize = value_size;
         }
         total_size += value_size;
      }
      char *log_str = (char *)malloc(total_size);
      memcpy(log_str, &(key.data_meta), header_size);
      char *ptr = log_str + header_size;
      *((int8_t *)ptr) = (int8_t)operation_type;
      *((uint16_t *)(ptr + 1)) = (uint16_t)db_id;
      memcpy(ptr + 3, key.get_data(), key_size);
      if (operation_type == SN_PUT) {
         memcpy(ptr + 3 + key_size, value.get_data(), value_size);
      }
      PROFILER_BEGIN("write migrate log");
      writer->write(log_str, total_size);
      PROFILER_END();
      free(log_str);
   }

   lsn_type update_log::find_log_tail()
   {
      lsn_type last_check_point_lsn = get_flsn();
      lsn_type restart_lsn = last_check_point_lsn > MIN_LSN ? last_check_point_lsn : MIN_LSN;
      log_file *file;
      do {
         file = file_manager->find_log_file(restart_lsn);
         if (file == NULL) {
            if (last_check_point_lsn == 0 && restart_lsn == MIN_LSN)
               return INVALID_LSN;
            if (restart_lsn != 0 && file_manager->find_log_file(restart_lsn - 1))
               break;
            log_error("can not find lsn:llu%", restart_lsn);
         }
         restart_lsn = file->full() ? file->get_end_lsn() : max(restart_lsn, file->get_tail_hint());
      }while(file->full());

      return restart_lsn;
   }

   lsn_type update_log::remove_incomplete_log_entry(lsn_type tail_lsn)
   {
      log_file *file = file_manager->find_log_file(tail_lsn);
      if (file == NULL) {
         return tail_lsn;
      } else {
         lsn_type start_lsn = file->get_start_lsn();
         if (tail_lsn == MIN_LSN) {
            return MIN_LSN;
         }

         uint size = file->get_size();
         char *buff = (char *)malloc(LOG_READ_BUFF_SIZE);
         uint32_t offset = tail_lsn - start_lsn + LOG_PAGE_HDR_SIZE;
         uint32_t read_len = file->read(buff, LOG_READ_BUFF_SIZE, offset);
         lsn_type next_lsn = tail_lsn;
         lsn_type temp_lsn = 0;
         uint32_t buf_offset  = 0;

         while(offset + buf_offset < size ){
            if (read_len == 0) break;

            if (read_len - buf_offset >= LOG_RECORD_HDR_TSIZE) {
               char *ptr = buff + buf_offset;
               item_meta_info *rec_header = (item_meta_info *)(ptr);
               uint16_t key_size = rec_header->keysize;
               uint32_t val_size = rec_header->valsize;
               uint32_t total_size = LOG_RECORD_HDR_TSIZE + key_size + val_size;
               if (total_size <= read_len - buf_offset) {
                  buf_offset += total_size;
                  temp_lsn = next_lsn;
                  next_lsn = next_lsn + total_size;
               } else {
                  offset += buf_offset;
                  if (offset  + total_size > size) break;
                  read_len = file->read(buff, LOG_READ_BUFF_SIZE, offset);
                  buf_offset = 0;

               }
            }else {
               offset += buf_offset;
               if (offset + LOG_RECORD_HDR_TSIZE > size) break;
               read_len = file->read(buff, LOG_READ_BUFF_SIZE, offset);
               buf_offset = 0;
            }
         }
         free(buff);
         file->truncate(next_lsn - start_lsn + LOG_PAGE_HDR_SIZE);
         return next_lsn;
      }
   }

   void update_log::close() 
   {
      if (file_manager) {
         file_manager->close();
         file_manager = NULL;
      }
      delete [] base_name;
      delete reader;
      reader = NULL;
      delete writer;
      writer = NULL;
      ctr_file->close();
      ctr_file = NULL;
      delete this;
   }

   log_scan_hander* update_log::get_next(log_scan_hander *handle)
   {
      return reader->get_next(handle);
   }

   log_scan_hander* update_log::begin_scan(int db_id, lsn_type start_lsn, lsn_type end_lsn)
   {
      return reader->begin_scan(db_id, start_lsn, end_lsn);
   }

   void update_log::end_scan(log_scan_hander *handle)
   {
      reader->reset();
      return reader->end_scan(handle);
   }

   log_file* update_log::switch_log_file(lsn_type start_lsn)
   {
      lsn_type min_lsn = get_hlsn();
      log_file *file = file_manager->find_reused_log_file(min_lsn);

      if(file == NULL) {
         file = file_manager->create_new_file(start_lsn);
      } else {
         file->reset(start_lsn);
      }
      return file;
   }

   log_file_manager::log_file_manager(const char *base_name, uint32_t size)
      : current_log_file(NULL), base_name(base_name)
   {
      file_size = size;
   }

   log_file_manager::~log_file_manager() 
   {
   }

   void log_file_manager::reset()
   {
      if (log_files.empty()){
         create_new_file(MIN_LSN);
         current_log_file = log_files[0];
      }
      for (uint32_t i = 0; i < log_files.size(); ++i){
         log_files[i]->restart();
         log_files[0]->set_start_lsn(MIN_LSN);
         log_files[0]->set_end_lsn(MIN_LSN + FILE_PAYLOAD);
         current_log_file = log_files[0];
      }
   }

   void log_file_manager::set_current_log_file(lsn_type lsn)
   {
      log_file *log_file = find_log_file(lsn);
      current_log_file = log_file;
   }

   log_file* log_file_manager::get_current_log_file()
   {
      return current_log_file;
   }

   lsn_type log_file_manager::init_on_first_open ()
   {
      create_new_file(MIN_LSN);
      current_log_file = log_files[0];
      return MIN_LSN;
   }

   std::string log_file_manager::make_file_name(uint32_t index)
   {
      return make_file_name(base_name.c_str(), index);
   }

   std::string log_file_manager::make_file_name(const char* basename, uint32_t index)
   {
      std::stringstream ss;
      ss << basename << "." << index;
      return ss.str();
   }

   void log_file_manager::set_log(update_log *log) 
   {
      this->log = log;
   }

   log_file_manager* log_file_manager::open(update_log *log, const char *base_name, uint32_t log_file_number, uint32_t current_log_file_number, uint32_t file_size)
   {
      log_file_manager *lfm = new log_file_manager(base_name, file_size);
      lfm->set_log(log);

      if(current_log_file_number == 0) {
         //lfm->createNewFile(MIN_LSN);
         return lfm;
      }
      bool is_file_exist = false;
      for (uint32_t i = 0; i < current_log_file_number; ++i) {
         if (access(lfm->make_file_name(i).c_str(), 0) == 0){
            is_file_exist = true;
         } else {
            log_error("log file %d not exist", i);
         }
      }

      for (uint32_t i = 0; i < current_log_file_number; ++i) {
         log_file* file = log_file::open(lfm->make_file_name(i).c_str(), O_RDWR);
         lfm->log_files.push_back(file);
      }
      return lfm;
   }
   void log_file_manager::close()
   {
      for (uint32_t i = 0; i < log_files.size(); ++i){
         log_files[i]->write_control_page();
         log_files[i]->close();
      }
      log_files.clear();
      delete this;
   }

   log_file* log_file_manager::find_log_file(lsn_type lsn, bool is_lock)
   {
      if (is_lock){
         mutex.lock();
      }
      log_file *file = NULL;
      for (uint i = 0; i < log_files.size(); ++i) {
         if (lsn >= log_files[i]->get_start_lsn() && lsn < log_files[i]->get_end_lsn(true)) {
            file = log_files[i];
         }
      }
      if (is_lock){
         mutex.unlock();
      }
      return file;
   }

   log_file* log_file_manager::find_reused_log_file(lsn_type cp_lsn)
   {
      for (uint i = 0; i < log_files.size(); ++i) {
         if (log_files[i]->get_end_lsn() <= cp_lsn)
            return log_files[i];
      }
      return NULL;
   }

   log_file* log_file_manager::create_new_file(lsn_type start_lsn)
   {
      log_debug("create log file timestamp: %llu", start_lsn);
      mutex.lock();
      size_t file_num = log_files.size();
      std::string  file_name = make_file_name((uint32_t)file_num);
      log_file *file = log_file::open(file_name.c_str(), O_CREAT | O_RDWR);
      file->set_start_lsn(start_lsn);
      file->set_end_lsn(start_lsn + FILE_PAYLOAD);
      file->write_control_page();
      log_files.push_back(file);
      mutex.unlock();
      return file;
   }

   log_file* log_file_manager::switch_read_new_file(lsn_type next_lsn)
   {
      lsn_type start_lsn = 0, temp = 0;
      uint32_t index = (uint32_t)(-1);
      for (uint32_t i = 0; i < log_files.size(); i++) {
         temp = log_files[i]->get_start_lsn();
         if(start_lsn == 0 && temp > next_lsn) {
            start_lsn = temp;
            index = i;
         }
         if (temp >= next_lsn && temp < start_lsn) {
            start_lsn = temp ;
            temp = start_lsn;
            index = i;
         }
      }
      if (index == (uint32_t)(-1)) {
         return NULL;
      } else {
         return log_files[index];
      }
   }

   uint32_t log_file::read(char *buff, uint32_t size, uint32_t offset)
   {
      return (uint32_t)(file->read((void*) buff, (size_t)size, (off_t) offset));
   }

   bool log_file::write(char *buff, uint32_t size, uint32_t offset)
   {
      return file->pwrite((void*) buff, (size_t)size, (off_t) offset);
   }

   void log_file::close()
   {
      delete file;
      file = NULL;
      free(ctrl_page);
      ctrl_page = NULL;
      delete this;
   }

   bool log_file::flush()
   {
      write_control_page();
      return file->sync();
   }

   bool log_file::full() {
      return (bool)ctrl_page->full;
   }

   void log_file::truncate(uint32_t size)
   {
      bool reT = file->truncate((off_t)size);
      if (!reT) {
         log_error("truncate file failed");
      }
   }
   //reuse log file
   void log_file::reset(lsn_type startLsn)
  {
      log_debug("reset log file startLsn: %llu", startLsn);
      set_start_lsn(startLsn);
      set_end_lsn(startLsn  + FILE_PAYLOAD);
      set_tail_hint(startLsn);
      full(false);
      truncate(LOG_PAGE_HDR_SIZE);
      write_control_page();
   }

   void log_file::restart()
   {
      memset(ctrl_page, 0, LOG_PAGE_HDR_SIZE);
      write_control_page();
   }

   void log_file::full(bool isFull) 
   {
      ctrl_page->full = isFull;
   }

   uint32_t log_file::get_size() const 
   {
      return (uint32_t)file->get_size();
   }

   lsn_type log_file::get_end_lsn(bool is_lock)
   {
      if (is_lock){
         end_lsn_lock.rdlock();
         lsn_type temp_lsn  = ctrl_page->end;
         end_lsn_lock.unlock();
         return temp_lsn;
      } else {
         return ctrl_page->end;
      }
   }

   void log_file::set_end_lsn(lsn_type lsn) 
   {
      end_lsn_lock.wrlock();
      ctrl_page->end = lsn;
      end_lsn_lock.unlock();
   }

   lsn_type log_file::get_start_lsn()
   {
      return ctrl_page->start;
   }

   void log_file::set_start_lsn(lsn_type lsn) 
   {
      ctrl_page->start = lsn;
   }

   void log_file::set_tail_hint(lsn_type lsn) 
   {
      ctrl_page->tail_hint = lsn;
   }

   lsn_type log_file::get_tail_hint() const
   {
      return ctrl_page->tail_hint;
   }

   void log_file::write_control_page() 
   {
      bool rc = file->pwrite(ctrl_page, LOG_PAGE_HDR_SIZE, 0);
      if(!rc) {
         log_error("write ctrl page failed");
      }
   }

   void log_file::read_control_page()
   {
      uint32_t read_size = file->read(ctrl_page, LOG_PAGE_HDR_SIZE, 0);
      if (read_size != LOG_PAGE_HDR_SIZE) {
         log_error("readCtrlPage error");
      }
   }

   log_file::log_file(const char *filename):ctrl_page(NULL)
   {
      ctrl_page = (log_file_control_page *)malloc(LOG_PAGE_HDR_SIZE);
      memset(ctrl_page, 0, LOG_PAGE_HDR_SIZE);
      file = new file_operation();
   }

   log_file* log_file::open(const char *file_name, int flags)
   {
      log_file* new_file = new log_file(file_name);
      bool ret = new_file->file->open((char *)file_name, flags, 0600);
      if (!ret)
         log_error("Open log file %s failed.", file_name);
      if ((flags & O_CREAT) == 0){
         new_file->read_control_page();
         assert(new_file->get_size() >= LOG_PAGE_HDR_SIZE);
      }
      return new_file;
   }

   log_file::~log_file() 
   {
      free(ctrl_page);
      delete file;
   }

   log_writer::log_writer(update_log *log, lsn_type lsn)
   {
      this->log = log;
      write_next = lsn;
      write_last = INVALID_LSN;
      flush_last = lsn;
      cur_log_file = log->file_manager->get_current_log_file();
      offset = lsn - cur_log_file->get_start_lsn() + LOG_PAGE_HDR_SIZE;
   }

   bool log_writer::write(char *data, uint size)
   {
      write_lock.wrlock();
     _Restart:
      lsn_type next_lsn = write_next + size;

      if (next_lsn > cur_log_file->get_end_lsn()) {
         lsn_type end_lsn = write_next;
         cur_log_file->set_end_lsn(end_lsn);
         cur_log_file->full(true);
         cur_log_file->flush();
         log->set_flsn(end_lsn);

         cur_log_file = log->switch_log_file(cur_log_file->get_end_lsn());
         write_next = cur_log_file->get_start_lsn();
         offset = LOG_PAGE_HDR_SIZE;
         goto _Restart;
      }

      bool ret;
      {
         ret = write_simple_log(data, size);
         write_last = write_next;
         write_next = write_next + size;
         write_lock.unlock();
      }
      return ret;
   }

   bool log_writer::write_simple_log(char *data , uint size)
   {
      if (offset == 0) {
         offset = LOG_PAGE_HDR_SIZE;
      }

      if (offset > log->file_size/2) {
         cur_log_file->set_tail_hint(write_next);
         cur_log_file->flush();
         log->set_flsn(write_next);
      }

      bool ret = cur_log_file->write(data, size, offset);
      if (ret == false) {
         log_error("write log error!");
         return false;
      }
      //m_curLogFile->flush();
      offset += size;
      return true;
   }

   void log_writer::set_current_file(log_file *file)
   {
      cur_log_file = file;
   }

   log_file* log_writer::get_current_file() 
   {
      return cur_log_file;
   }

   void log_writer::set_offset(uint32_t offset) 
   {
      offset = offset;
   }

   uint32_t log_writer::get_offset()
   {
      return offset;
   }

   lsn_type log_writer::get_next_write_lsn()
   {
      lsn_type lsn;
      write_lock.rdlock();
      lsn = write_next;
      write_lock.unlock();;
      return lsn;
   }

   log_reader::log_reader(log_read_buffer *buff, uint32_t size)
   {
      buffer = buff;
      this->size = size;
   }

   void log_reader::set_offset(uint32_t offset)
   {
      buffer->set_offset(offset);
   }

   void log_reader::set_current_file(log_file *file)
   {
      buffer->set_current_file(file);
   }

   void log_reader::reset()
   {
      buffer->reset();
   }

   log_reader::~log_reader()
   {
      delete buffer;
   }

   log_scan_hander* log_reader::begin_scan(int bucketno, lsn_type start_lsn, lsn_type endLsn) const
   {
      start_lsn = max(start_lsn, MIN_LSN);
      assert(start_lsn <= endLsn);
      log_scan_hander *handler = new log_scan_hander();
      handler->end_lsn = endLsn;
      handler->lsn = start_lsn;
      handler->next_lsn = start_lsn;
      handler->db_id = bucketno;
      handler->log_entry = NULL;
      return handler;
   }

   log_scan_hander* log_reader::get_next(log_scan_hander *handle)
   {
      lsn_type to_read;
      to_read = handle->next_lsn;
      if (to_read >= handle->end_lsn) {
         handle->log_entry = NULL;
         return handle;
      }
      handle->lsn = handle->next_lsn;
      log_record_entry *cur_entry = get_next(handle->db_id, handle->next_lsn, handle->end_lsn, &to_read);
      handle->next_lsn = to_read;
      handle->log_entry = cur_entry;
      return handle;
   }

   log_record_entry* log_reader::get_next(int db_id, lsn_type lsn, lsn_type end_lsn, lsn_type *next_lsn)
   {
      return buffer->get_next(db_id, lsn, end_lsn, next_lsn);
   }

   void log_reader::end_scan(log_scan_hander *handle)
   {
      delete handle;
   }

   log_read_buffer::log_read_buffer(log_file_manager *file_manager, uint this_size)
   {
      file_mgr = file_manager;
      has_read = false;
      offset = 0;
      current_file = NULL;
      buffer_offset = 0;
      read_length = 0;
      want_length = 0;
      last_length = 0;
      buffer = (char *)malloc(this_size);
      if(buffer == NULL) {
         log_error("LogReadBuffer alloc buff failed!");
      }
      size = this_size;
   }

   void log_read_buffer::reset()
   {
      has_read = false;
      read_length = 0;
      want_length = 0;
      last_length = 0;
      offset = 0;
      buffer_offset = 0;
   }

   void log_read_buffer::set_offset(uint32_t offset)
   {
      offset = offset;
   }

   void log_read_buffer::set_current_file(log_file *file) 
   {
      current_file = file;
   }

   uint32_t log_read_buffer::get_offset ()
   {
      return offset;
   }

   uint32_t log_read_buffer::get_buffer_offset() 
   {
      return buffer_offset;
   }

   log_record_entry* log_read_buffer::get_next(int db_id, lsn_type lsn, lsn_type end_lsn, lsn_type *nlsn)
   {
      assert(end_lsn > lsn);
      if (has_read == false) {
         current_file = file_mgr->find_log_file(lsn, true);
         offset = lsn - current_file->get_start_lsn() + LOG_PAGE_HDR_SIZE;
         uint64_t lsn_offset = (uint64_t)(end_lsn - lsn);
         uint read_size =(uint)(size > lsn_offset ? lsn_offset : size);
         read_length = current_file->read(buffer, read_size, offset);
         has_read = true;
      }

      if(lsn >= current_file->get_end_lsn(true)){
         current_file = file_mgr->find_log_file(lsn, true);
         buffer_offset = 0;
         offset = LOG_PAGE_HDR_SIZE;
         uint64_t lsn_offset = (uint64_t)(end_lsn - lsn);
         uint read_size =(uint)(size > lsn_offset ? lsn_offset : size);
         read_length = current_file->read(buffer, read_size, offset);
      }

     _Restart:

      if (read_length - buffer_offset >= LOG_RECORD_HDR_TSIZE) {
         char *ptr = buffer + buffer_offset;
         uint8_t op = *((uint8_t *)(ptr + LOG_RECORD_HDR_SIZE));
         item_meta_info *rec_header = (item_meta_info *)(ptr);

         uint16_t key_size = rec_header->keysize;
         assert(key_size>2);
         uint32_t val_size = 0;
         if (op == (uint8_t)SN_PUT){
            val_size = rec_header->valsize;
         }
         uint32_t total_size = LOG_RECORD_HDR_TSIZE + key_size + val_size;

         if (total_size <= read_length - buffer_offset) {
            log_record_entry *rec = new log_record_entry();
            ptr += LOG_RECORD_HDR_TSIZE;
            data_entry key(ptr, key_size, true);
            data_entry value;
            if (op == (uint8_t)SN_PUT) {
               ptr += key_size;
               value.set_data(ptr, val_size, true);
            }
            rec->header = *rec_header;
            rec->operation_type = op;
            rec->key = key;
            rec->value = value;
            buffer_offset += total_size;
            *nlsn = lsn + total_size;
            return rec;
         } else {
            offset += buffer_offset;
            uint64_t lsn_offset = (uint64_t)(end_lsn - lsn);
            uint tsize = (uint)(size > lsn_offset ? lsn_offset : size);
            read_length = current_file->read(buffer, tsize, offset);
            buffer_offset = 0;
            goto _Restart;
         }
      }else {
         offset += buffer_offset;
         uint64_t lsn_offset = (uint64_t)(end_lsn - lsn);
         uint tsize = (uint)(size > lsn_offset ? lsn_offset : size);
         read_length = current_file->read(buffer, tsize, offset);
         buffer_offset = 0;
         goto _Restart;
      }
      return NULL;
   }

   log_read_buffer::~log_read_buffer() 
   {
      if (buffer != NULL){
         free(buffer);
      }
   }

   lsn_type log_scan_hander::current_lsn() const
   {
      return lsn;
   }
   const log_record_entry* log_scan_hander::get_log_entry() const 
   {
      return log_entry;
   }
   lsn_type log_scan_hander::get_next_lsn() const
   {
      return next_lsn;
   }

   string control_file::file_name = "fdb.ctrl";

   lsn_type control_file::get_hlsn()
   {
      return ctrl_file_hdr->m_hlsn;
   }

   void control_file::set_hlsn(lsn_type lsn)
   {
      ctrl_file_hdr->m_hlsn = lsn;
      write_control_file();
   }

   lsn_type control_file::get_flush_lsn()
   {
      return ctrl_file_hdr->m_flsn;
   }

   void control_file::set_flush_lsn(lsn_type lsn)
   {
      ctrl_file_hdr->m_flsn = lsn;
      write_control_file();
   }

   control_file* control_file::open(const char *path)
   {
      file_operation *file = new file_operation();
      control_file *ctrl_file = new control_file(file);
      char *buff = new char[CTRL_FILE_HDR_SIZE];
      memset(buff, 0, CTRL_FILE_HDR_SIZE);
      std::stringstream ss;
      ss << path << "/" << control_file::file_name;
      string new_path = ss.str();

      if(access(new_path.c_str(), 0) == 0) {
         ctrl_file->ctrl_file->open((char *)new_path.c_str(), O_RDWR, 0600);
         ctrl_file->ctrl_file->pread(buff, CTRL_FILE_HDR_SIZE, 0);
         ctrl_file->ctrl_file_hdr = ((control_file_header *)buff);
         return ctrl_file;
      } else{
         ctrl_file->ctrl_file->open((char *)new_path.c_str(), O_CREAT |O_RDWR, 0600);
         ctrl_file->ctrl_file_hdr = ((control_file_header *)buff);
         ctrl_file->write_control_file();
         return ctrl_file;
      }
   }
   
   void control_file::reset() {
     memset(ctrl_file_hdr, 0, CTRL_FILE_HDR_SIZE);
     write_control_file();
   }
   
   void control_file::close()
   {
      write_control_file();
      delete [] ctrl_file_hdr;
      ctrl_file_hdr = NULL;
      delete ctrl_file;
      ctrl_file = NULL;
      delete this;
   }

   bool control_file::write_control_file()
   {
      return ctrl_file->pwrite(ctrl_file_hdr, CTRL_FILE_HDR_SIZE, 0);
   }

   control_file::control_file(file_operation *file)
   {
      ctrl_file = file;
      ctrl_file_hdr = NULL;
   }

   void update_log::set_flsn(lsn_type lsn)
   {
      ctr_file->set_flush_lsn(lsn);

   }

   lsn_type update_log::get_flsn()
   {
      return ctr_file->get_flush_lsn();
   }

   lsn_type update_log::get_hlsn()
   {
      return ctr_file->get_hlsn();
   }

   void update_log::set_hlsn(lsn_type lsn)
   {
      ctr_file->set_hlsn(lsn);
   }

}
