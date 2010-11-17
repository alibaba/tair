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
#ifndef TABLE_BUILDER1_H
#define TABLE_BUILDER1_H
#include "table_builder.hpp"

namespace tair {
  namespace config_server {
    using namespace std;
    class table_builder1:public table_builder {
    public:
      table_builder1(uint32_t bucket_count, uint32_t copy_count);
      ~table_builder1();
    protected:
      int is_this_node_OK(server_id_type node_id, int line_num,
                          size_t node_idx, hash_table_type & hash_table_dest,
                          int option_level, bool node_in_use = false);
      void caculate_capable();
      int get_tokens_per_node(const server_id_type &)
      {
        return tokens_per_node_min;
      }
    private:
      int tokens_per_node_min;        //try my bset to make every data server handles tokens_per_node_min
      //or tokens_per_node_min + 1 buckets
      int tokens_per_node_max_count;        // how many data server handle tokenPerNode_min + 1 buckets
      int tokens_per_node_min_count;        // how many data server handles tokenPerNode_min buckets
      int master_tokens_per_node_min;
      int master_tokens_per_node_max_count;
      int master_tokens_per_node_min_count;
    };
  }
}
#endif
