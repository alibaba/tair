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
#ifndef TAIR_STAT_INFO_H
#define TAIR_STAT_INFO_H
#include <vector>
#include <map>
#include "tbsys.h"
#include "tbnet.h"
#include "define.hpp"
namespace tair {
  namespace config_server {
    using namespace std;
    class stat_info_detail {
    public:
      enum
      {
        GETCOUNT = 0,
        PUTCOUNT,
        EVICTCOUNT,
        REMOVECOUNT,
        HITCOUNT,
        DATASIZE,
        USESIZE,
        ITEMCOUNT,
        //READBYTES,
        //WRITEBYTES,
        //CURRLOAD,
      };
        stat_info_detail()
      {
      }
      size_t get_unit_size() const
      {
        return data_holder.size();
      }
      uint64_t get_unit_value(uint32_t unit_index) const
      {
        return unit_index >= get_unit_size() ? 0 : data_holder[unit_index];
      }
      int64_t set_unit_value(uint32_t unit_index, int64_t v);
      int64_t add_unit_value(uint32_t unit_index, int64_t v);
      void update_stat_info_detail(const stat_info_detail & sv);
      void insert_stat_info_detail(const stat_info_detail & sv);
      void clear()
      {
        data_holder.clear();
      }
      void format_detail(const char *prefix, std::map<std::string, std::string> &m_k_v) const;

      void encode(tbnet::DataBuffer * output) const;
      void decode(tbnet::DataBuffer * input);
    private:
        std::vector<int64_t> data_holder;
    };
    class node_stat_info {
    public:
      node_stat_info():last_update_time(0)
      {
      }
      node_stat_info(const node_stat_info & rv):last_update_time(rv.
                                                                 last_update_time),
        data_holder(rv.data_holder)
      {
      }
      node_stat_info & operator =(const node_stat_info & rv);
      void set_last_update_time(uint32_t time)
      {
        last_update_time = time;
      }
      uint32_t get_last_update_time() const
      {
        return last_update_time;
      }
      std::map<uint32_t, stat_info_detail> get_stat_data() const
      {
        return data_holder;
      }
      //insert will delete the original value
      //update will add this value to original value, if original value not exist, this will be equal to insert
      void insert_stat_detail(uint32_t area, const stat_info_detail & detail)
      {
        data_holder[area].insert_stat_info_detail(detail);
      }
      void update_stat_info(uint32_t area, const stat_info_detail & detail)
      {
        data_holder[area].update_stat_info_detail(detail);
      }
      void update_stat_info(const node_stat_info & rv);

      void clear()
      {
        data_holder.clear();
      }

      void encode(tbnet::DataBuffer * output) const;

      void decode(tbnet::DataBuffer * input);
      void format_info(std::map<std::string, std::string> &m_k_v) const;
    private:
      uint32_t last_update_time;        //this value only for config server
      std::map<uint32_t, stat_info_detail> data_holder;        //<area, infoDetail>
    };
  }
}
#endif
