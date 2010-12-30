/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_DUMP_FILTER
#define TAIR_DUMP_FILTER
#include "data_entry.hpp"
#include "fdb/file_op.hpp"
namespace tair {
  namespace storage {
    using namespace tair::common;
    class dump_thread;
    class dump_filter {
    public:
      dump_filter();
      dump_filter(const dump_filter &);
      void set_parameter(uint32_t start_time, uint32_t end_time, int32_t area,
                         const std::string & dir);
      bool operator <(const dump_filter & rv) const;

    private:
      //this key should been decodeArea
        bool do_dump(const data_entry & key, const data_entry & value,
                     uint32_t area);
      void end_dump(bool cancle);
      void turn_interval(uint32_t now);
    private:
        bool should_be_kept(uint32_t area, uint32_t modify_time) const;
      uint32_t start_time;
      uint32_t end_time;
      uint32_t now;
      int32_t area;                //-1 means all
      std::string out_data_dir;
    private:
      fdb::file_operation file_op;
      std::string file_name;
      friend class dump_thread;
    };
  }
}
#endif
