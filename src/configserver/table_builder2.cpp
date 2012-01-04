/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * table_builder2.cpp is the implement of table build strategy which make the data safety
 * is the first priority
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "table_builder2.hpp"
#include <stdio.h>
#include <stdlib.h>
//#define TDEBUG
namespace tair {
  namespace config_server {
    int table_builder2::is_this_node_OK(server_id_type node_id, int line_num,
                                        size_t node_idx,
                                        hash_table_type & hash_table_dest,
                                        int option_level, bool node_in_use)
    {
      if(is_node_availble(node_id) == false)
        return INVALID_NODE;
      int turn = 0;
      int tokenPerNode_min =
        (node_id.second ==
         max_machine_room_id) ? mtokens_per_node_min : otokens_per_node_min;
      int tokenPerNode_max_count =
        (node_id.second ==
         max_machine_room_id) ? mtokens_per_node_max_count :
        otokens_per_node_max_count;
      if(node_in_use == true)
        turn = 1;
      if(line_num == 0) {
        if(mtokens_count_in_node[node_id] >=
           master_server_capable[node_id] + turn) {
          return TOOMANY_MASTER;
        }
      }
      else if(option_level == CONSIDER_ALL) {
        if(tokens_count_in_node[node_id] >= server_capable[node_id] + turn) {
          return TOOMANY_BUCKET;
        }
      }
      else {
        if(option_level != CONSIDER_FORCE &&
           tokens_count_in_node_now[node_id] >= tokenPerNode_min + turn &&
           max_count_now >= tokenPerNode_max_count + turn) {
          return TOOMANY_BUCKET;
        }
      }
      if(line_num == 0 && option_level == CONSIDER_BASE) {
        return NODE_OK;
      }
      set<uint32_t> pos_limit;
      pos_limit.insert(node_id.second);
      for(size_t i = 0; i < copy_count; i++) {
        if((int) i != line_num) {
          pos_limit.insert(hash_table_dest[i][node_idx].second);
          if(hash_table_dest[i][node_idx].first == node_id.first) {
            return SAME_NODE;
          }
          //if (option_level < CONSIDER_BASE) {
          //    if ( hashTable_dest[i][node_idx].second == node_id.second){
          //        return SAME_POS;
          //    }
          //}
        }
      }
      if(build_stat_normal && line_num != 0 && pos_limit.size() < 2)
        return SAME_POS;
      return NODE_OK;
    }

    void table_builder2::set_available_server(const set <
                                              node_info * >&ava_server)
    {
      available_server.clear();
      map<uint32_t, int>pos_count;
      for(set<node_info *>::const_iterator it = ava_server.begin();
          it != ava_server.end(); it++) {
        log_info("mask %"PRI64_PREFIX"u:%s & %"PRI64_PREFIX"u -->%"PRI64_PREFIX"u",
            (*it)->server->server_id, tbsys::CNetUtil::addrToString((*it)->server->server_id).c_str(),
            pos_mask, (*it)->server->server_id & pos_mask);
        available_server.
          insert(make_pair
                 ((*it)->server->server_id,
                  (*it)->server->server_id & pos_mask));
        (pos_count[(*it)->server->server_id & pos_mask])++;
      }
      pos_max = 0;
      for(map<uint32_t, int>::iterator it = pos_count.begin();
          it != pos_count.end(); ++it) {
        log_info("pos:%d count:%d", it->first, it->second);
        if(it->second > pos_max) {
          pos_max = it->second;
          max_machine_room_id = it->first;
        }
      }
      int diff_server = available_server.size() - pos_max - pos_max;
      if(diff_server <= 0)
        diff_server = 0 - diff_server;
      float ratio = diff_server / (float) pos_max;
      if(ratio > stat_change_ratio || available_server.size() < copy_count
         || ratio > 0.9999) {
        build_stat_normal = false;
      }
      else {
        build_stat_normal = true;
      }
      log_warn("diff_server = %d ratio = %f stat_change_ratio=%f",
               diff_server, ratio, stat_change_ratio);
    }
    void table_builder2::caculate_capable()
    {
      int available_server_count = available_server.size();
      int real_copy_count = copy_count;

      if(copy_count <= 1) {
        build_stat_normal = false;
      }
      if(build_stat_normal == false) {
        if(real_copy_count > 2) {
          real_copy_count = 2;
        }

      }
      int mtoken_per_node_min;
      int mtoken_per_node_max_count;
      int mtoken_per_node_min_count;
      int otoken_per_node_min;
      int otoken_per_node_max_count;
      int otoken_per_node_min_count;
      int mmaster_token_per_node_min;
      int mmaster_token_per_node_max_count;
      int mmaster_token_per_node_min_count;
      int omaster_token_per_node_min;
      int omaster_token_per_node_max_count;
      int omaster_token_per_node_min_count;


      assert(build_stat_normal);
      int token_per_node_min =
        bucket_count * real_copy_count / available_server_count;
      int token_per_node_max_count =
        bucket_count * real_copy_count -
        token_per_node_min * available_server_count;
      int token_per_node_min_count =
        available_server_count - token_per_node_max_count;

      int master_token_per_node_min = bucket_count / available_server_count;
      int master_token_per_node_max_count =
        bucket_count - master_token_per_node_min * available_server_count;
      int master_token_per_node_min_count =
        available_server_count - master_token_per_node_max_count;

      if(build_stat_normal) {
        assert(real_copy_count >= 2);

        int total_bucket = bucket_count * (real_copy_count - 1);
        int master_bucket =
          (int) (bucket_count * ((float) pos_max / available_server_count));
        int server_count = pos_max;

        int balance_bucket =
          (int) (bucket_count * real_copy_count *
                 ((float) pos_max / available_server_count));

        if(total_bucket > balance_bucket) {
          total_bucket = balance_bucket;
        }
        mtoken_per_node_min = total_bucket / server_count;
        mtoken_per_node_max_count =
          total_bucket - mtoken_per_node_min * server_count;
        mtoken_per_node_min_count = server_count - mtoken_per_node_max_count;

        mmaster_token_per_node_min = master_bucket / server_count;
        mmaster_token_per_node_max_count =
          master_bucket - mmaster_token_per_node_min * server_count;
        mmaster_token_per_node_min_count =
          server_count - mmaster_token_per_node_max_count;


        total_bucket = bucket_count * real_copy_count -
          (mtoken_per_node_min * mtoken_per_node_min_count +
           (mtoken_per_node_min + 1) * mtoken_per_node_max_count);
        master_bucket =
          bucket_count -
          (mmaster_token_per_node_min * mmaster_token_per_node_min_count +
           (mmaster_token_per_node_min +
            1) * mmaster_token_per_node_max_count);
        server_count = available_server_count - server_count;

        otoken_per_node_min = total_bucket / server_count;
        otoken_per_node_max_count =
          total_bucket - otoken_per_node_min * server_count;
        otoken_per_node_min_count = server_count - otoken_per_node_max_count;

        omaster_token_per_node_min = master_bucket / server_count;
        omaster_token_per_node_max_count =
          master_bucket - omaster_token_per_node_min * server_count;
        omaster_token_per_node_min_count =
          server_count - omaster_token_per_node_max_count;
      }

      server_capable.clear();
      master_server_capable.clear();
      {
        token_per_node_min = mtoken_per_node_min;
        token_per_node_max_count = mtoken_per_node_max_count;
        token_per_node_min_count = mtoken_per_node_min_count;

        master_token_per_node_min = mmaster_token_per_node_min;
        master_token_per_node_max_count = mmaster_token_per_node_max_count;
        master_token_per_node_min_count = mmaster_token_per_node_min_count;

        log_info("node in max room");
        log_info("tokenPerNode_min=%d", token_per_node_min);
        log_info("tokenPerNode_max_count=%d", token_per_node_max_count);
        log_info("tokenPerNode_min_count=%d", token_per_node_min_count);

        log_info("masterTokenPerNode_min_count=%d",
                 master_token_per_node_min_count);
        log_info("masterTokenPerNode_max_count=%d",
                 master_token_per_node_max_count);
        log_info("masterTokenPerNode_min_count=%d",
                 master_token_per_node_min_count);

        int max_s = 0;
        int mmax_s = 0;
        int i = 0;
        for(server_list_type::const_iterator it = available_server.begin();
            it != available_server.end(); it++) {
          if((*it).second != max_machine_room_id)
            continue;
          if(max_s < token_per_node_max_count) {
            server_capable[*it] = token_per_node_min + 1;
            max_s++;
          }
          else {
            server_capable[*it] = token_per_node_min;
          }

          if(mmax_s < master_token_per_node_max_count) {
            master_server_capable[*it] = master_token_per_node_min + 1;
            mmax_s++;
          }
          else {
            master_server_capable[*it] = master_token_per_node_min;
          }
          i++;
        }
      }
      {
        token_per_node_min = otoken_per_node_min;
        token_per_node_max_count = otoken_per_node_max_count;
        token_per_node_min_count = otoken_per_node_min_count;

        master_token_per_node_min = omaster_token_per_node_min;
        master_token_per_node_max_count = omaster_token_per_node_max_count;
        master_token_per_node_min_count = omaster_token_per_node_min_count;

        log_info("node not in max room");
        log_info("tokenPerNode_min=%d", token_per_node_min);
        log_info("tokenPerNode_max_count=%d", token_per_node_max_count);
        log_info("tokenPerNode_min_count=%d", token_per_node_min_count);

        log_info("masterTokenPerNode_min_count=%d",
                 master_token_per_node_min_count);
        log_info("masterTokenPerNode_max_count=%d",
                 master_token_per_node_max_count);

        int max_s = 0;
        int mmax_s = 0;
        int i = 0;
        for(server_list_type::const_iterator it = available_server.begin();
            it != available_server.end(); it++) {
          if((*it).second == max_machine_room_id)
            continue;
          if(max_s < token_per_node_max_count) {
            server_capable[*it] = token_per_node_min + 1;
            max_s++;
          }
          else {
            server_capable[*it] = token_per_node_min;
          }

          if(mmax_s < master_token_per_node_max_count) {
            master_server_capable[*it] = master_token_per_node_min + 1;
            mmax_s++;
          }
          else {
            master_server_capable[*it] = master_token_per_node_min;
          }
          i++;
        }
      }

    }
    int table_builder2::
      rebuild_table(const hash_table_type & hash_table_source,
                    hash_table_type & hash_table_dest, bool no_quick_table)
    {
      if(!build_stat_normal) {
        int diff_server = available_server.size() - pos_max;
        float ratio = diff_server / (float) pos_max;
        log_error
          ("can not build table diff_server = %d ratio = %f stat_change_ratio=%f",
           diff_server, ratio, stat_change_ratio);
        return BUILD_ERROR;

      }
      return table_builder::rebuild_table(hash_table_source, hash_table_dest,
                                          no_quick_table);
    }

  }
}
