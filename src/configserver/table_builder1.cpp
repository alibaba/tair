/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * table_builder1.cpp is the implement of table build strategy which make the load balance
 * is the first priority
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "table_builder1.hpp"
#include <stdio.h>
#include <stdlib.h>
//#define TDEBUG
namespace tair {
  namespace config_server {
    table_builder1::table_builder1(uint32_t b_c,
                                   uint32_t c_c):table_builder(b_c, c_c)
    {
    }
    table_builder1::~table_builder1()
    {
    }

    int table_builder1::is_this_node_OK(server_id_type node_id, int line_num,
                                        size_t node_idx,
                                        hash_table_type & hash_table_dest,
                                        int option_level, bool node_in_use)
    {
      if(is_node_availble(node_id) == false)
        return INVALID_NODE;
      int turn = 0;
      if(node_in_use == true)
        turn = 1;
      if(line_num == 0) {
        if(mtokens_count_in_node[node_id] >=
           master_server_capable[node_id] + turn) {
          return TOOMANY_MASTER;
        }
      }
      else if(option_level == CONSIDER_ALL) { //line_num != 0
        if(tokens_count_in_node[node_id] >= server_capable[node_id] + turn) {
          return TOOMANY_BUCKET;
        }
      }
      else { //line_num != 0 && option_level != CONSIDER_ALL
        if(option_level != CONSIDER_FORCE &&
           tokens_count_in_node_now[node_id] >= tokens_per_node_min + turn &&
           max_count_now >= tokens_per_node_max_count + turn) {
          return TOOMANY_BUCKET;
        }
      }
      if(line_num == 0 && option_level == CONSIDER_BASE) {
        return NODE_OK;
      }
      for(size_t i = 0; i < copy_count; i++) {
        if((int) i != line_num) {
          if(hash_table_dest[i][node_idx].first == node_id.first) {
            return SAME_NODE;
          }
          if(option_level < CONSIDER_BASE) { //CONSIDER_ALL
            if(hash_table_dest[i][node_idx].second == node_id.second) {
              return SAME_POS;
            }
          }
        }
      }
      return NODE_OK;
    }

    void table_builder1::caculate_capable()
    {
      int S = available_server.size();

      tokens_per_node_min = bucket_count * copy_count / S;
      tokens_per_node_max_count =
        bucket_count * copy_count - tokens_per_node_min * S;
      tokens_per_node_min_count = S - tokens_per_node_max_count;

      master_tokens_per_node_min = bucket_count / S;
      master_tokens_per_node_max_count =
        bucket_count - master_tokens_per_node_min * S;
      master_tokens_per_node_min_count = S - master_tokens_per_node_max_count;

      log_debug("bucket_count: %u\n copy_count: %u\n tokens_per_node_min: %d\n tokens_per_node_max_count: %d\n "
          "tokens_per_node_min_count: %d\n master_tokens_per_node_min: %d\n master_tokens_per_node_max_count: %d\n "
          "master_tokens_per_node_min_count: %d\n",
          bucket_count, copy_count, tokens_per_node_min, tokens_per_node_max_count, tokens_per_node_min_count,
          master_tokens_per_node_min, master_tokens_per_node_max_count, master_tokens_per_node_min_count);

      server_capable.clear();
      master_server_capable.clear();
      int max_s = 0;
      int mmax_s = 0;
      int i = 0;
      int pre_node_count_min = 0;
      int pre_mnode_count_min = 0;
      int size = 0;
      int sum = 0;

      for(map<server_id_type, int >::iterator it =
          tokens_count_in_node.begin(); it != tokens_count_in_node.end();
          it++) {
        if(it->second != 0) {
          size++;
          sum += it->second;
        }
      }
      if(size > 0) {
        pre_node_count_min = sum / size;
      }
      size = sum = 0;
      for(map<server_id_type, int>::iterator it =
          mtokens_count_in_node.begin(); it != mtokens_count_in_node.end();
          it++) {
        if(it->second != 0) {
          size++;
          sum += it->second;
        }
      }
      if(size > 0) {
        pre_mnode_count_min = sum / size;
      }

      log_debug("pre_node_count_min: %d, pre_mnode_count_min: %d",
          pre_node_count_min, pre_mnode_count_min);

      for(server_list_type::const_iterator it = available_server.begin();
          it != available_server.end(); it++) {
        int last_node_count = tokens_count_in_node[*it];
        //log_debug("will caculate server %s:%d last token is %d ",
        //    tbsys::CNetUtil::addrToString(it->first).c_str(), it->first, last_node_count);

        //log_debug("pre_node_count_min = %d pre_mnode_count_min=%d",pre_node_count_min, pre_mnode_count_min);
        if(last_node_count <= pre_node_count_min) {        //try my best to make every data sever handle tokenPerNode_min buckets.
          //log_debug("try to make ist min");
          int min_s = i - max_s;
          if(min_s < tokens_per_node_min_count) {
            server_capable[*it] = tokens_per_node_min;
          }
          else {
            server_capable[*it] = tokens_per_node_min + 1;
            max_s++;
          }
        }
        else {
          //log_debug("try to make ist max");
          if(max_s < tokens_per_node_max_count) {
            server_capable[*it] = tokens_per_node_min + 1;
            max_s++;
          }
          else {
            server_capable[*it] = tokens_per_node_min;
          }
        }

        int last_mnode_count = mtokens_count_in_node[*it];
        if(last_mnode_count <= pre_mnode_count_min) {        //try my best to make every data sever handle tokenPerNode_min buckets.
          int mmin_s = i - mmax_s;
          if(mmin_s < master_tokens_per_node_min_count) {
            master_server_capable[*it] = master_tokens_per_node_min;
          }
          else {
            master_server_capable[*it] = master_tokens_per_node_min + 1;
            mmax_s++;
          }
        }
        else {
          if(mmax_s < master_tokens_per_node_max_count) {
            master_server_capable[*it] = master_tokens_per_node_min + 1;
            mmax_s++;
          }
          else {
            master_server_capable[*it] = master_tokens_per_node_min;
          }
        }
        i++;
      }

    }


  }
}
