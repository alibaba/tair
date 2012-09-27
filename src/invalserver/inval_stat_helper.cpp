/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * statistics impl
 *
 * Version: $Id: inval_stat_helper.cpp 949 2012-08-15 08:39:54Z fengmao.pj@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "inval_stat_helper.hpp"
namespace tair {
  inval_stat_helper inval_stat_helper::inval_stat_helper_instance;

  inval_stat_helper::inval_stat_helper()
  {
    last_update_time = tbsys::CTimeUtil::getTime();
    compressed_data = NULL;
    compressed_data_size = 0;
    group_count = 0;
    atomic_set(&work_now, WAIT);
    atomic_set(&need_compressed, DATA_UNCOMPRESSED);
    stat = NULL;
    current_stat = NULL;
  }
  inval_stat_helper::~inval_stat_helper()
  {
    _stop = true;
    wait();
    if (compressed_data != NULL) {
      delete [] compressed_data;
      compressed_data = NULL;
    }
    if (stat != NULL) {
      delete [] stat;
    }
    if (current_stat != NULL) {
      delete [] current_stat;
    }
    if (compressed_data_buffer != NULL) {
      delete [] compressed_data_buffer;
    }
  }

  void inval_stat_helper::run(tbsys::CThread *thread, void *arg)
  {
    //can't work ,waiting for the parameters `group_names
    while (atomic_read(&work_now) == WAIT) {
      log_info("wating for group names information");
      TAIR_SLEEP(_stop, 5);
    }
    log_info("inval_stat_helper starts working...");
    while (_stop == false) {
      //reset the state.
      reset();
      //clean the compressed flag
      atomic_set(&need_compressed,DATA_UNCOMPRESSED);
      //sleep
      TAIR_SLEEP(_stop, 10);
    }
  }

  // not threadsafe, make sure there is only
  // one thread calling this
  void inval_stat_helper::reset()
  {
    //lock
    tbsys::CThreadGuard guard(&mutex);
    //clean current_stat
    for (int i = 0; i < group_count; ++i) {
      inval_group_stat *group_stat_value = current_stat + i;
      group_stat_value->reset();
    }
    //swap
    inval_group_stat *temp = stat;
    stat = current_stat;
    current_stat = temp;
    group_stat_map.clear();
    for (int i = 0; i < group_count; ++i) {
      inval_group_stat *group_stat_value = stat + i;
      group_stat_value->reset();
      group_stat_value->set_group_name(group_names[i]);
      group_stat_map.insert(group_stat_map_t::value_type(group_names[i], group_stat_value));
    }
    //compute the ratio.
    uint64_t now = tbsys::CTimeUtil::getTime();
    uint64_t interval = now - last_update_time;
    last_update_time = now;
    interval /= 1000000; // conver to second
    if (interval == 0) interval = 1;
    log_info("inval server start calculate ratio, interval: %d", interval);
    //for every group, every area and every type.
    for (int i = 0; i < group_count; i++) {
      inval_group_stat *gs = current_stat + i;
      for (int k = 0; k < TAIR_MAX_AREA_COUNT; ++k) {
        inval_area_stat  &as = gs->get_area_stat(k);
        for (int i = 0; i < inval_area_stat::STAT_ELEM_COUNT; ++i) {
          as.set_invalid_count(i, as.get_invalid_count(i) / interval);
          as.set_hide_count(i, as.get_hide_count(i) / interval);
          as.set_prefix_invalid_count(i, as.get_prefix_invalid_count(i) / interval);
          as.set_prefix_hide_count(i, as.get_prefix_hide_count(i) / interval);
        }
      }
    }
    //unlock
  }

  bool inval_stat_helper::do_compress() {
    if (compressed_data != NULL) {
      compressed_data_size = 0;
      delete [] compressed_data;
    }
    unsigned long data_size = compressed_data_buffer_size;
    unsigned long uncompressed_data_size = group_count * sizeof(inval_group_stat);
    int ret = compress(compressed_data_buffer, &data_size,
        (unsigned char*)current_stat, uncompressed_data_size);
    if (ret == Z_OK) {
      compressed_data = new char [data_size];
      if (compressed_data == NULL) {
        log_error("[FATAL ERROR] failed to allocate compressed_data.");
        return false;
      }
      else {
        memcpy(compressed_data, compressed_data_buffer, data_size);
        compressed_data_size = data_size;
        log_info("compress inval server stats done (%d=>%d)", uncompressed_data_size, compressed_data_size);
      }
    }
    else {
      log_error("[FATAL ERROR] failed to uncompress the data.");
      return false;
    }
    //set flag
    atomic_set(&need_compressed,DATA_COMPRESSED);
    return true;
  }

  // the buffer be allocated by `get_stat_buffer,freed by invaker.
  // return true, if successful; otherwise, return false.
  bool inval_stat_helper::get_stat_buffer(char*& buffer, unsigned long &buffer_size,
      unsigned long & uncompressed_data_size, int &group_count_out)
  {
    if (atomic_read(&work_now) == WAIT) {
      buffer_size = 0;
      if (buffer != NULL) {
        delete [] buffer;
        buffer = NULL;
      }
      log_error("wait for group names.");
      return false;
    }
    tbsys::CThreadGuard guard(&mutex);
    if (atomic_read(&need_compressed) == DATA_UNCOMPRESSED) {
      if (do_compress() == false) {
        log_error("[FATAL ERROR] failed to compress the statistic data!");
        return false;
      }
    }
    //copy stat data into user's buffer.
    if (compressed_data == NULL) {
      buffer_size = 0;
      buffer = NULL;
      uncompressed_data_size = 0;
      return false;
    }
    buffer = new char [compressed_data_size];
    if (buffer == NULL) {
      buffer_size = 0;
      buffer= NULL;
      log_error("[FATAL ERROR] fail to alloc memory .");
      return false;
    }
    memcpy(buffer, compressed_data, compressed_data_size);
    buffer_size = compressed_data_size;
    uncompressed_data_size = compressed_data_buffer_size;
    group_count_out = group_count;
    return true;
  }

  bool inval_stat_helper::setThreadParameter(const std::vector<std::string> &group_names_input)
  {
    tbsys::CThreadGuard guard(&mutex);
    if (group_names_input.empty() ) {
      log_error("[FATAL ERROR] group_count must be more than 0.");
      return false;
    }
    group_count = group_names_input.size();
    int group_stat_data_size = group_count * sizeof(inval_group_stat);
    //alloc memory for statists information.
    stat = new inval_group_stat[group_count];
    if (stat == NULL) {
      log_error("[FATAL ERROR] fail to alloc memory for 'stat' ");
      return false;
    }
    current_stat = new inval_group_stat[group_count];
    if (stat == NULL) {
      log_error("[FATAL ERROR] fail to alloc memory for 'current_stat'");
      return false;
    }
    memset(stat, 0, group_stat_data_size);
    memset(current_stat, 0, group_stat_data_size);
    group_stat_map.clear();
    group_names.clear();
    for (int i = 0; i < group_count; ++i) {
      inval_group_stat* group_stat_value = stat + i;
      //set group's name.
      group_stat_value->set_group_name(group_names_input[i]);
      group_stat_map.insert(group_stat_map_t::value_type(group_names_input[i], group_stat_value));
      group_names.push_back(group_names_input[i]);
    }
    //alloc memroy for compressing stat data
    unsigned long stat_data_len = group_count * sizeof(inval_group_stat);
    compressed_data_buffer_size = compressBound(stat_data_len);
    compressed_data_buffer = new unsigned char [compressed_data_buffer_size];
    if (compressed_data_buffer == NULL) {
      log_error("[FATAL ERROR] fail to alloc memory for 'compressed_data_buffer'");
      return false;
    }
    //now we can work
    atomic_set(&work_now, WORK);
    return true;
  }

  void inval_stat_helper::statistcs(const uint32_t operation_name,
      const std::string& group_name,
      const uint32_t area,
      const uint32_t op_type) {
    group_stat_map_t::iterator it = group_stat_map.find(group_name);
    if (it != group_stat_map.end()) {
      switch(operation_name) {
        case INVALID:
          it->second->get_area_stat(area).inc_invalid_count(op_type);
          break;
        case PREFIX_INVALID:
          it->second->get_area_stat(area).inc_prefix_invalid_count(op_type);
          break;
        case HIDE:
          it->second->get_area_stat(area).inc_hide_count(op_type);
          break;
        case PREFIX_HIDE:
          it->second->get_area_stat(area).inc_prefix_hide_count(op_type);
          break;
        default:
          log_error("[FATAL ERROR] unknown operation_name=%d,area=%d,op_type=%d", operation_name, area, op_type);
      }
    } //end of if
    else {
      log_error("[FATAL ERROR] can't find the group name: %s ,in stat.", group_name.c_str());
    }
  }
}
