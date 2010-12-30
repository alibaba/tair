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
#ifndef TAIR_FDB_MIGRATE_LOG_HPP
#define TAIR_FDB_MIGRATE_LOG_HPP

#include <stdio.h>
#include <vector>
#include <tbsys.h>
#include "data_entry.hpp"
#include "file_op.hpp"

namespace tair {
   using namespace tair::storage::fdb;
   using namespace tair::common;

   class log_scan_hander;
   class        log_file;
   class log_reader;
   class log_read_buffer;
   class log_writer;
   class log_file_manager;

#define INVALID_LSN             ((uint64_t)(-1))
   typedef uint64_t lsn_type;
   static const uint LOG_READ_BUFF_SIZE = 2 * 1024 * 1024;
   static const uint LOG_FIZE_SIZE = 64 * 1024 * 1024;

   struct control_file_header{
      lsn_type m_hlsn;
      lsn_type m_flsn;
   };
   static const size_t CTRL_FILE_HDR_SIZE = sizeof(control_file_header);

   class control_file{
   public:
      lsn_type get_flush_lsn();
      void set_flush_lsn(lsn_type lsn);
      lsn_type get_hlsn();
      void set_hlsn(lsn_type lsn);

      static control_file* open(const char *path);
      void close();
      void reset();
      bool write_control_file();
   private:
      static std::string file_name;
      control_file(file_operation *file);
      file_operation *ctrl_file;
      control_file_header *ctrl_file_hdr;
   };

   typedef enum {
      SN_PUT = 1,
      SN_REMOVE
   }sn_operation_type;


   struct log_record_entry {
      uint8_t operation_type;
      item_meta_info header;
      data_entry key;
      data_entry value;
   };

   struct log_record_header{
      item_meta_info header;
      uint8_t operation_type;
      uint16_t block_number;
   };


   static const size_t LOG_RECORD_HDR_SIZE = sizeof(item_meta_info);
   static const size_t LOG_RECORD_HDR_TSIZE = LOG_RECORD_HDR_SIZE + 3;
   struct log_file_control_page {
      uint64_t  checksum;
      lsn_type  start;
      lsn_type  end;
      lsn_type tail_hint;
      uint32_t          size;
      uint8_t           full;
      uint8_t           pad[3];
   };

   template<typename T> inline T align_round(T v) {
      return (v + 7) & ~7;
   }

   static const size_t LOG_PAGE_HDR_SIZE = align_round(offsetof(log_file_control_page, pad));
   static const uint64_t MIN_LSN = sizeof(log_file_control_page);
   static const uint64_t FILE_PAYLOAD = LOG_FIZE_SIZE - LOG_PAGE_HDR_SIZE;

   class update_log {
      friend class log_writer;
   public:
      static update_log* open(const char *dir, const char *name, uint32_t log_file_number, bool is_migrating, uint32_t file_size = LOG_FIZE_SIZE);
      void close();
      void log(sn_operation_type operation_type, data_entry &key, data_entry &value, uint16_t db_id);
      lsn_type remove_incomplete_log_entry(lsn_type tail_lsn);
      log_file* switch_log_file(lsn_type lsn);

      log_scan_hander* get_next(log_scan_hander *scan_handler);
      log_scan_hander* begin_scan(int db_id, lsn_type start_lsn, lsn_type end_lsn);
      void end_scan(log_scan_hander *scan_handler);
      lsn_type get_current_lsn();
      void set_flsn(lsn_type lsn);
      lsn_type get_flsn();
      lsn_type get_hlsn();
      void set_hlsn(lsn_type lsn);
   private:
      update_log(bool is_migrating);
      ~update_log();
      void start_up(bool is_migrating);
      static uint32_t scan_log_directory(const char *dir, const char *name);
      static int filter(const struct dirent* dirp, const char *name);
      lsn_type find_log_tail();
      void init();
      char *base_name;
      uint32_t log_file_number;
      uint32_t current_log_file_number;
      tbsys::CThreadCond  mutex;
      log_writer *writer;
      log_reader *reader;
      log_file_manager *file_manager;
      uint32_t file_size;
      bool is_migrating;
      control_file *ctr_file;

   };
   class log_file_manager {
   public:
      static log_file_manager* open(update_log *log, const char *base_name, uint32_t log_file_number, uint32_t current_log_file_number, uint32_t size);
      log_file* find_reused_log_file(lsn_type lsn);
      log_file* create_new_file(lsn_type lsn);
      log_file* find_log_file(lsn_type lsn, bool is_lock = false);
      log_file* switch_read_new_file(lsn_type lsn);
      void close();
      void set_current_log_file(lsn_type lsn);
      log_file* get_current_log_file();
      static std::string make_file_name(const char *base_name, uint32_t index);
      lsn_type init_on_first_open ();
      void reset();
   private:
      ~log_file_manager();
      log_file_manager(const char *base_name, uint32_t size);
      std::string make_file_name(uint32_t index);
      void set_log(update_log *log);

   private:
      std::vector<log_file *> log_files;
      tbsys::CThreadCond  mutex;
      log_file *current_log_file;
      uint32_t file_size;
      std::string base_name;
      uint32_t file_number;
      update_log *log;
   };

   class log_file {
   public:
      static log_file* open(const char *file_name, int flags);
      void close();
      bool flush();
      bool full();

      void set_tail_hint(lsn_type lsn);
      lsn_type get_tail_hint() const;
      log_file* switch_log_file();

      lsn_type get_end_lsn(bool is_lock = false);
      void set_end_lsn(lsn_type lsn);
      void set_start_lsn(lsn_type lsn);
      lsn_type get_start_lsn();
      void reset(lsn_type lsn);
      void restart();
      uint32_t get_size() const;
      void full(bool is_full);
      void write_control_page();
      uint32_t read(char *buff, uint32_t size, uint32_t offset);
      bool write(char *buff, uint32_t size, uint32_t offset);
      void truncate(uint32_t size);
      log_file(const char *file_name);
      void read_control_page();
   private:

      ~log_file();

   private:

      file_operation *file;
      log_file_control_page *ctrl_page;
      tbsys::CRWSimpleLock      end_lsn_lock;
   };

   class log_writer {
   public:
      log_writer(update_log *log, lsn_type lsn);
      bool write(char *data, uint size);
      bool write_simple_log(char *data , uint size);
      void set_current_file(log_file *file);
      log_file* get_current_file();
      void set_offset(uint32_t offset);
      uint32_t get_offset();
      lsn_type get_next_write_lsn();

   private:
      lsn_type flush_last;
      tbsys::CRWSimpleLock      flush_lock;
      lsn_type write_last;
      tbsys::CRWSimpleLock      write_lock;
      lsn_type write_next;

      log_file *cur_log_file;
      update_log *log;
      uint offset;
   };

   class log_reader {
   public:
      log_reader(log_read_buffer *buff, uint size = LOG_READ_BUFF_SIZE);
      ~log_reader();
      void set_current_file(log_file *file);
      void set_offset(uint32_t offset);

      log_scan_hander* begin_scan(int bucket_number, lsn_type start_lsn, lsn_type end_lsn) const;
      log_scan_hander* get_next(log_scan_hander *handle);
      void      end_scan(log_scan_hander *handle);
      log_record_entry* get_next(int db_id, lsn_type start_lsn, lsn_type end_lsn, lsn_type *next_lsn);
      void reset();
   private:
      log_read_buffer *buffer;
      uint32_t size;
   };

   class log_read_buffer {
   public:
      log_read_buffer(log_file_manager *file_manager, uint size = LOG_READ_BUFF_SIZE);
      ~log_read_buffer();
      log_record_entry* get_next(int db_id, lsn_type lsn, lsn_type end_lsn, lsn_type *next_lsn);
      void set_current_file(log_file *file);
      void set_offset(uint32_t offset);
      uint32_t get_offset();
      uint32_t get_buffer_offset();
      void reset();

   private:
      uint32_t offset;
      uint32_t buffer_offset;
      uint32_t want_length;
      uint32_t last_length;
      log_file *current_file;
      char *buffer;
      log_file_manager *file_mgr;
      bool has_read;
      uint32_t read_length;
      uint size;
   };
   class log_scan_hander {
      friend class log_reader;
      friend class update_log;
   public:
      const log_record_entry* get_log_entry() const;
      lsn_type current_lsn() const;
   private:
      log_scan_hander() {
      }
      lsn_type get_next_lsn() const;
   private:
      int db_id;
      lsn_type lsn;
      lsn_type next_lsn;
      lsn_type end_lsn;
      log_record_entry *log_entry;
   };
   struct log_file_hasher {
      inline lsn_type operator() (log_file *file) const {
         return file->get_start_lsn();
      }
   };
   struct log_file_equaler {
      inline bool operator() (lsn_type startLsn, log_file *file) const {
         return file->get_start_lsn() == startLsn;
      }
   };

}
#endif
