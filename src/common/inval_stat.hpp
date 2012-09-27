/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * data structure used for stat
 *
 * Version: $Id: inval_stat.hpp 470 2012-08-15 05:26:28Z fengmao.pj@taobao.com$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef INVAL_STAT_H
#define INVAL_STAT_H
#include <tbsys.h>
#include <tbnet.h>

#include <vector>
#include <string>

#include "define.hpp"
#include "log.hpp"

namespace tair {
  /*
   *  The statistics infomation of the namespace will be recorded.
   */
  class inval_area_stat {
  public:
    enum { FIRST_EXEC = 0, RETRY_EXEC = 1, FINALLY_EXEC = 2, STAT_ELEM_COUNT = 3 };
    inval_area_stat()
    {
      reset();
    }

    //get, inc, set operations for `invalid count value.
    inline int get_invalid_count(const uint32_t op_type)
    {
      return atomic_read(&invalid_count_value[op_type]);
    }

    inline void inc_invalid_count(const uint32_t op_type)
    {
      atomic_inc(&invalid_count_value[op_type]);
    }

    inline void set_invalid_count(const uint32_t op_type, int value)
    {
      atomic_set(&invalid_count_value[op_type],value);
    }

    //get, inc, set operations for `hide count value
    inline int get_hide_count(const uint32_t op_type)
    {
      return atomic_read(&hide_count_value[op_type]);
    }

    inline void inc_hide_count(const uint32_t op_type)
    {
      atomic_inc(&hide_count_value[op_type]);
    }

    inline void set_hide_count(const uint32_t op_type, int value)
    {
      atomic_set(&hide_count_value[op_type],value);
    }

    //get, inc, set operations for `prefix invalid count value
    inline int get_prefix_invalid_count(const uint32_t op_type)
    {
      return atomic_read(&prefix_invalid_count_value[op_type]);
    }

    inline void inc_prefix_invalid_count(const uint32_t op_type)
    {
      atomic_inc(&prefix_invalid_count_value[op_type]);
    }

    inline void set_prefix_invalid_count(const uint32_t op_type, int value)
    {
      atomic_set(&prefix_invalid_count_value[op_type],value);
    }

    //get, inc, set,prefix hide count value
    inline int get_prefix_hide_count(const uint32_t op_type)
    {
      return atomic_read(&prefix_hide_count_value[op_type]);
    }

    inline void inc_prefix_hide_count(const uint32_t op_type)
    {
      atomic_inc(&prefix_hide_count_value[op_type]);
    }

    inline void set_prefix_hide_count(const uint32_t op_type, int value)
    {
      atomic_set(&prefix_hide_count_value[op_type],value);
    }

    //clean up
    inline void reset()
    {
      atomic_set(&invalid_count_value[FIRST_EXEC], 0);
      atomic_set(&invalid_count_value[RETRY_EXEC], 0);
      atomic_set(&invalid_count_value[FINALLY_EXEC], 0);

      atomic_set(&hide_count_value[FIRST_EXEC], 0);
      atomic_set(&hide_count_value[RETRY_EXEC], 0);
      atomic_set(&hide_count_value[FINALLY_EXEC], 0);

      atomic_set(&prefix_invalid_count_value[FIRST_EXEC], 0);
      atomic_set(&prefix_invalid_count_value[RETRY_EXEC], 0);
      atomic_set(&prefix_invalid_count_value[FINALLY_EXEC], 0);

      atomic_set(&prefix_hide_count_value[FIRST_EXEC], 0);
      atomic_set(&prefix_hide_count_value[RETRY_EXEC], 0);
      atomic_set(&prefix_hide_count_value[FINALLY_EXEC], 0);
    }

  private:
    //stats the execution of opertions.
    atomic_t invalid_count_value[STAT_ELEM_COUNT];
    atomic_t hide_count_value[STAT_ELEM_COUNT];
    atomic_t prefix_invalid_count_value[STAT_ELEM_COUNT];
    atomic_t prefix_hide_count_value[STAT_ELEM_COUNT];

  }; //end of inval_area_stat

  /*
   * group statistics information, which contain `TAIR_MAX_AREA_COUNT areas' information.
   */
  class inval_group_stat {
  public:
    inval_group_stat()
    {
    }

    inline char* get_group_name()
    {
      return group_name;
    }

    inline void set_group_name(const std::string groupname)
    {
      int copy_size = groupname.size();
      copy_size = (copy_size > MAX_GROUP_NAME_SIZE) ? MAX_GROUP_NAME_SIZE : copy_size;
      strncpy(group_name, groupname.c_str(), copy_size);
      group_name[copy_size] = '\0';
    }

    inline inval_area_stat & get_area_stat(const uint32_t index)
    {
      if (index < 0 || index > TAIR_MAX_AREA_COUNT) {
        log_error("[FATAL ERROR] area must be in the range of [0, TAIR_MAX_AREA_COUNT]");
        return area_stat[0];
      }
      return area_stat[index];
    }

    inline void reset()
    {
      memset((char*)area_stat, 0, TAIR_MAX_AREA_COUNT*sizeof(inval_area_stat));
    }
  public:
    inval_area_stat  area_stat[TAIR_MAX_AREA_COUNT];
    static const int MAX_GROUP_NAME_SIZE = 255;
    char group_name[MAX_GROUP_NAME_SIZE + 1];
  };//end of inval_group_stat

  //used for tair client, cplusplus version
  struct inval_stat_data_t {
    int group_count;
    char *stat_data;

    inval_stat_data_t() : group_count(0), stat_data(NULL)
    {
    }
    ~inval_stat_data_t()
    {
      if (stat_data != NULL) {
        delete [] stat_data;
        stat_data = NULL;
      }
    }
  };
} //end of the namesapace
#endif
