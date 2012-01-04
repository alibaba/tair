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
#ifndef TABLE_BUILDER2_H
#define TABLE_BUILDER2_H
#include "table_builder.hpp"
namespace tair {
  namespace config_server {
    class table_builder2:public table_builder {
    public:
      table_builder2(float change_ratio, uint32_t bucket_c, uint32_t copy_c)
      : table_builder(bucket_c, copy_c)
      {
        stat_change_ratio = change_ratio;
      }
       ~table_builder2()
      {
      }
    public:
      void set_available_server(const set<node_info *>&ava_ser);
      int is_this_node_OK(server_id_type node_id, int line_num,
                          size_t node_idx, hash_table_type & hash_table_dest,
                          int option_level, bool node_in_use = false);
      void caculate_capable();
      int get_tokens_per_node(const server_id_type & node_id)
      {
        return (node_id.second ==
                max_machine_room_id) ? mtokens_per_node_min :
          otokens_per_node_min;
      }
      int rebuild_table(const hash_table_type & hash_table_source,
                        hash_table_type & hash_table_dest,
                        bool no_quick_table = false);
    private:
      bool build_stat_normal;
      float stat_change_ratio;
      int pos_max;                // max (count in each rack)
      uint32_t max_machine_room_id;

      //we will divided all data server to two room.
      //the data servers in the rack which has the most data servers in is the max room
      //prefix M_ for it.
      //prefix O_ for the others.

      int mtokens_per_node_min;        //try my bset to make every data server handles tokens_per_node_min
      //or tokens_per_node_min + 1 buckets
      int mtokens_per_node_max_count;        // how many data server handle tokenPerNode_min + 1 bucket
      int mtokens_per_node_min_count;        // how many data server handles tokenPerNode_min buckets

      int otokens_per_node_min;
      int otokens_per_node_max_count;
      int otokens_per_node_min_count;

    };
  }
}
#endif
