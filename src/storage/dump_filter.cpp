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
#include "dump_filter.hpp"
namespace tair {
  namespace storage {
    using namespace std;
    dump_filter::dump_filter()
    {
      now = start_time = end_time = 0;
      area = -1;

    }
    dump_filter::dump_filter(const dump_filter & rv)
    {
      start_time = rv.start_time;
      end_time = rv.end_time;
      now = rv.now;
      area = rv.area;
      out_data_dir = rv.out_data_dir;
    }
    void dump_filter::set_parameter(uint32_t start_time, uint32_t end_time,
                                    int32_t area, const string & dir)
    {
      if(start_time > end_time) {
        this->start_time = end_time;
        this->end_time = start_time;
      }
      else {
        this->start_time = start_time;
        this->end_time = end_time;
      }
      this->area = area;
      out_data_dir = dir;
    }
    void dump_filter::turn_interval(uint32_t now)
    {
      if(end_time > now) {
        end_time = now;
      }
      this->now = now;
    }
    bool dump_filter::operator <(const dump_filter & rv) const
    {
      if(area < rv.area)
        return true;
      return false;

    }
    bool dump_filter::do_dump(const data_entry & key,
                              const data_entry & value, uint32_t area)
    {
      if(should_be_kept(area, key.data_meta.mdate)) {
        if(!file_op.is_opened()) {
          int flags = O_RDWR | O_CREAT | O_LARGEFILE;
          char file_name[512];
          snprintf(file_name, 512, "%s", out_data_dir.c_str());
          if(!tbsys::CFileUtil::mkdirs(file_name)) {
            log_error("create dir error dir is %s", file_name);
            return false;
          }
          char str_s_time[20];
          char str_e_time[20];
          char str_n_time[20];
          tbsys::CTimeUtil::timeToStr(start_time, str_s_time);
          tbsys::CTimeUtil::timeToStr(end_time, str_e_time);
          tbsys::CTimeUtil::timeToStr(now, str_n_time);
          str_n_time[8] = 0;
          snprintf(file_name, 512, "%s/dump_area_%d_%s.dum",
                   out_data_dir.c_str(), area, str_n_time);
          this->file_name = file_name;
          snprintf(file_name, 512, "%s_tmp", this->file_name.c_str());
          if(!file_op.open(file_name, flags, 0644)) {
            return false;
          }
        }
        //this may be changed
        file_op.write(&area, sizeof(uint32_t));

        uint16_t int16_data = key.data_meta.version;
        file_op.write(&int16_data, sizeof(uint16_t));

        uint32_t int32_data = key.data_meta.cdate;
        file_op.write(&int32_data, sizeof(uint32_t));
        int32_data = key.data_meta.mdate;
        file_op.write(&int32_data, sizeof(uint32_t));
        int32_data = key.data_meta.edate;
        file_op.write(&int32_data, sizeof(uint32_t));

        int32_data = key.get_size();
        //log_debug("keysize = %u", int32_data);
        file_op.write(&int32_data, sizeof(uint32_t));
        if(int32_data)
          file_op.write(key.get_data(), int32_data);

        int32_data = value.get_size();
        //log_debug("valuesize = %u", int32_data);
        file_op.write(&int32_data, sizeof(uint32_t));
        if(int32_data)
          file_op.write(value.get_data(), int32_data);

      }
      return true;
    }
    void dump_filter::end_dump(bool cancle)
    {
      file_op.close();
      char file_name[512];
      snprintf(file_name, 512, "%s_tmp", this->file_name.c_str());
      if(cancle) {
        ::unlink(file_name);
      }
      else {
        rename(file_name, this->file_name.c_str());
      }
    }
    bool dump_filter::should_be_kept(uint32_t area, uint32_t modify_time) const
    {
      if(modify_time < start_time || modify_time > end_time)
        return false;
      return this->area == -1 || (int) area == this->area;
    }
  }
}
