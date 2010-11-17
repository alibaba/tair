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
#include "stat_info.hpp"

namespace {
  char *format_str[] = {
    "unknow",
    "getCount",
    "putCount",
    "evictCount",
    "removeCount",
    "hitCount",
    "dataSize",
    "useSize",
    "itemCount",
    // "readBytes",
    // "writeBytes",
    // "currLoad",
  };
}

namespace tair {
  namespace config_server {
    int64_t stat_info_detail::set_unit_value(uint32_t unit_index, int64_t v)
    {
      if(unit_index >= data_holder.size()) {
        data_holder.resize(unit_index + 1, 0);
      }
      data_holder[unit_index] = v;
        return v;
    }
    int64_t stat_info_detail::add_unit_value(uint32_t unit_index, int64_t v)
    {
      return set_unit_value(unit_index, v + get_unit_value(unit_index));
    }
    void stat_info_detail::
      update_stat_info_detail(const stat_info_detail & sv)
    {
      for(uint32_t i = 0; i < sv.data_holder.size(); i++) {
        add_unit_value(i, sv.data_holder[i]);
      }
    }
    void stat_info_detail::
      insert_stat_info_detail(const stat_info_detail & sv)
    {
      data_holder.resize(sv.get_unit_size(), 0);
      for(uint32_t i = 0; i < sv.data_holder.size(); i++) {
        set_unit_value(i, sv.data_holder[i]);
      }
    }
    void stat_info_detail::format_detail(const char *prefix,
                                         std::map<std::string, std::string > &m_k_v) const
    {
      char key[200];
      char value[50];
      for(uint32_t i = 0; i < data_holder.size(); i++)
      {
        uint32_t str_index =
          (i > (sizeof(format_str) / sizeof(char *) - 2)) ? 0 : i + 1;
          snprintf(key, 200, "%s %s", prefix, format_str[str_index]);
          snprintf(value, 50, "%" PRI64_PREFIX "u", data_holder[i]);
          m_k_v[key] = value;
      }
    }
    void stat_info_detail::encode(tbnet::DataBuffer * output) const
    {
      output->writeInt32(data_holder.size());
      for(uint i = 0; i < data_holder.size(); i++)
      {
        output->writeInt64(data_holder[i]);
      }

    }
    void stat_info_detail::decode(tbnet::DataBuffer * input)
    {
      data_holder.clear();
      uint32_t size = input->readInt32();
      for(uint32_t i = 0; i < size; i++) {
        data_holder.push_back(input->readInt64());
      }
    }
    node_stat_info & node_stat_info::operator =(const node_stat_info & rv)
    {
      if(&rv == this)
        return *this;
      last_update_time = rv.last_update_time;
      data_holder = rv.data_holder;
      return *this;
    }
    void node_stat_info::update_stat_info(const node_stat_info & rv)
    {
      for(std::map<uint32_t, stat_info_detail>::const_iterator it =
          rv.data_holder.begin(); it != rv.data_holder.end(); it++) {
        data_holder[it->first].update_stat_info_detail(it->second);
      }
      last_update_time = rv.last_update_time;
    }
    void node_stat_info::encode(tbnet::DataBuffer * output) const
    {
      output->writeInt32(data_holder.size());
      for(std::map<uint32_t, stat_info_detail>::const_iterator it =
          data_holder.begin(); it != data_holder.end(); it++)
      {
        output->writeInt32(it->first);
        it->second.encode(output);
      }

    }
    void node_stat_info::decode(tbnet::DataBuffer * input)
    {
      data_holder.clear();
      uint32_t size = input->readInt32();
      for(uint32_t i = 0; i < size; i++) {
        uint32_t area = input->readInt32();
        stat_info_detail info;
        info.decode(input);
        data_holder[area] = info;
      }
    }
    void node_stat_info::format_info(std::map<std::string,
                                     std::string> &m_k_v) const
    {
      std::map<uint32_t, stat_info_detail>::const_iterator it =
        data_holder.begin();
      char str_area[20];
      for(; it != data_holder.end(); it++)
      {
        snprintf(str_area, 20, "%u", it->first);
        it->second.format_detail(str_area, m_k_v);
      }
    }
  }
}
