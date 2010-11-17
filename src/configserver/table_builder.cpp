/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * table_builder.cpp is base of the two different table build strategies
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "table_builder.hpp"
#include <stdio.h>
#include <stdlib.h>
// #define TDEBUG
namespace tair {
  namespace config_server {
    table_builder::~table_builder()
    {
    }
    void table_builder::print_tokens_in_node()
    {
      map<server_id_type, int>::iterator it;
        log_debug("max_count_now =%d", max_count_now);
        log_debug("tokens");
      for(it = tokens_count_in_node.begin(); it != tokens_count_in_node.end();
          it++)
      {
        log_debug("S(%lld,%d)=%d ", it->first.first, it->first.second,
                  it->second);
      }
      log_debug("tokens_now");
      for(it = tokens_count_in_node_now.begin();
          it != tokens_count_in_node_now.end(); it++) {
        log_debug("S(%lld,%d)=%d ", it->first.first, it->first.second,
                  it->second);
      }
      log_debug("mtokes:");

      for(it = mtokens_count_in_node.begin();
          it != mtokens_count_in_node.end(); it++) {
        log_debug("S(%lld,%d)=%d ", it->first.first, it->first.second,
                  it->second);
      }
    }
    void table_builder::print_count_server()
    {
      map<int, server_list_type>::iterator it;
      log_debug("count:");
      for(it = count_server.begin(); it != count_server.end(); it--) {
        log_debug("%d:   ", it->first);
        for(server_list_type::iterator it2 = it->second.begin();
            it2 != it->second.end(); it2++) {
          log_debug("%lld,%d  ", it2->first, it2->second);
        }
      }
      log_debug("mcount:");
      for(it = mcount_server.begin(); it != mcount_server.end(); it++) {
        log_debug("%d:   ", it->first);
        for(server_list_type::iterator it2 = it->second.begin();
            it2 != it->second.end(); it2++) {
          log_debug("%lld,%d  ", it2->first, it2->second);
        }
      }
    }
    void table_builder::print_available_server()
    {
      log_debug("available server:");
      log_debug("    ");
      for(server_list_type::iterator it = available_server.begin();
          it != available_server.end(); it++) {
        log_debug("S:%s,%-3d  ",
                  tbsys::CNetUtil::addrToString(it->first).c_str(),
                  it->second);
      }
    }

    void table_builder::print_hash_table(hash_table_type & hash_table)
    {
      if(hash_table.empty())
        return;
      for(uint32_t j = 0; j < bucket_count; ++j) {
        char kk[64];
        sprintf(kk, "%u-->", j);
        string ss(kk);
        for(uint32_t i = 0; i < copy_count; ++i) {
          char str[1024];
          sprintf(str, "%s(%-3d)  ",
                  tbsys::CNetUtil::addrToString(hash_table[i][j].first).
                  c_str(), hash_table[i][j].second);
          ss += str;
        }
        log_debug("%s", ss.c_str());
      }
    }

    void table_builder::print_capabale()
    {
      server_capable_type::iterator it;
      log_debug("server capabale:");
      for(it = server_capable.begin(); it != server_capable.end(); it++) {
        log_debug("%lld,%d %d   ", it->first.first, it->first.second,
                  it->second);
      }
      for(it = master_server_capable.begin();
          it != master_server_capable.end(); it++) {
        log_debug("%lld,%d %d   ", it->first.first, it->first.second,
                  it->second);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////

    void table_builder::init_token_count(map<server_id_type, int>&collector)
    {
      collector.clear();
      server_list_type::iterator it;
      for(it = available_server.begin(); it != available_server.end(); it++) {
        collector[*it] = 0;
      }
    }

    bool table_builder::update_node_count(server_id_type node_id,
                                          map<server_id_type, int>& collector)
    {
      map<server_id_type, int>::iterator node_it = collector.find(node_id);
      if(node_it != collector.end()) {
        node_it->second++;
        return true;
      }
      return false;

    }

    void table_builder::build_index(const map<server_id_type,  int>&collector, map<int, server_list_type> &indexer)
    {
      map<server_id_type, int>::const_iterator it;
      indexer.clear();
      for(it = collector.begin(); it != collector.end(); it++) {
        indexer[it->second].insert(it->first);
      }
    }

    bool table_builder::is_node_availble(server_id_type node_id)
    {
      if(node_id.first == INVALID_FLAG)
        return false;
      server_list_type::iterator it = available_server.find(node_id);
      return it != available_server.end();
    }


    void table_builder::change_tokens_count_in_node(map<server_id_type, int>&count_in_node,
                                                    const server_id_type &node_id, 
                                                    map<int, server_list_type > &count_server_map,
                                                    map<int, server_list_type> &candidate_node_info,
                                                    server_capable_type & server_capable_info,
                                                    bool minus)
    {

      int &token_count_in_node = count_in_node[node_id];
      count_server_map[token_count_in_node].erase(node_id);
      candidate_node_info[token_count_in_node -
                          server_capable_info[node_id]].erase(node_id);
      if(minus)
        token_count_in_node--;
      else
        token_count_in_node++;

      count_server_map[token_count_in_node].insert(node_id);
      candidate_node_info[token_count_in_node -
                          server_capable_info[node_id]].insert(node_id);

    }

    void table_builder::invaliad_node(int line_num, size_t node_idx,
                                      hash_table_type & hash_table_data)
    {
      server_id_type node_id = hash_table_data[line_num][node_idx];
      if(is_node_availble(node_id)) {
        change_tokens_count_in_node(tokens_count_in_node, node_id,
                                    count_server, scandidate_node,
                                    server_capable, true);

        if(line_num == 0) {
          change_tokens_count_in_node(mtokens_count_in_node, node_id,
                                      mcount_server, mcandidate_node,
                                      master_server_capable, true);
        }
      }
      hash_table_data[line_num][node_idx].first = INVALID_FLAG;
    }
    bool table_builder::change_master_node(size_t idx,
                                           hash_table_type & hash_tble_dest,
                                           bool force_flag)
    {
      int chosen_line_num = -1;
      int min_node_count = -1;
      for(size_t next_line = 1; next_line < copy_count; next_line++) {
        server_id_type node_id = hash_tble_dest[next_line][idx];
        if(is_node_availble(node_id)) {
          int &mtoken_count_in_node = mtokens_count_in_node[node_id];
          if(min_node_count == -1 || min_node_count > mtoken_count_in_node) {
            chosen_line_num = next_line;
            min_node_count = mtoken_count_in_node;
          }
        }
      }
      if(min_node_count == -1) {
        return false;                // we lost all copys of this bucket
      }
      server_id_type & choosen_node_id = hash_tble_dest[chosen_line_num][idx];
      if(force_flag == false) {
        if(mtokens_count_in_node[choosen_node_id] >=
           master_server_capable[choosen_node_id]) {
          return false;
        }
      }
      server_id_type org_node_id = hash_tble_dest[0][idx];
      server_id_type new_master_node = choosen_node_id;
      // master of this bucket is chaned, so we must turn the stat info
      hash_tble_dest[0][idx] = new_master_node;
      tokens_count_in_node_now[new_master_node]++;
      change_tokens_count_in_node(mtokens_count_in_node, new_master_node,
                                  mcount_server, mcandidate_node,
                                  master_server_capable, false);

      if(is_node_availble(org_node_id)) {
        choosen_node_id = org_node_id;
        change_tokens_count_in_node(mtokens_count_in_node, org_node_id,
                                    mcount_server, mcandidate_node,
                                    master_server_capable, true);
      }
      else {
        choosen_node_id.first = INVALID_FLAG;
      }
      return true;
    }
    void table_builder::update_node(int line_num, size_t node_idx,
                                    const server_id_type & suitable_node,
                                    hash_table_type & hash_table_dest)
    {
      hash_table_dest[line_num][node_idx] = suitable_node;
      if(is_node_availble(suitable_node)) {
        change_tokens_count_in_node(tokens_count_in_node, suitable_node,
                                    count_server, scandidate_node,
                                    server_capable, false);
        if(line_num == 0) {
          change_tokens_count_in_node(mtokens_count_in_node, suitable_node,
                                      mcount_server, mcandidate_node,
                                      master_server_capable, false);
        }
      }
    }
    void table_builder::init_candidate(map < int,
                                       server_list_type > &candidate_node,
                                       server_capable_type * pcapable,
                                       map<int, server_list_type> *pcount_server)
    {
      candidate_node.clear();
      for(map<int, server_list_type>::iterator it = pcount_server->begin();
          it != pcount_server->end(); it++) {
        for(server_list_type::iterator server_it = it->second.begin();
            server_it != it->second.end(); server_it++) {
          candidate_node[it->first -
                         (*pcapable)[*server_it]].insert(*server_it);
        }
      }
    }

    table_builder::server_id_type table_builder::
      get_suitable_node(int line_num, size_t node_idx,
                        hash_table_type & hash_table_dest,
                        server_id_type & original_node)
    {
      map <server_id_type, int>*ptokens_node;
      map <int, server_list_type> *pcount_server;
      server_capable_type *pcapable;
      map<int, server_list_type> *pcandidate_node;
      if(line_num == 0) {
        ptokens_node = &mtokens_count_in_node;
        pcount_server = &mcount_server;
        pcapable = &master_server_capable;
        pcandidate_node = &mcandidate_node;
      }
      else {
        ptokens_node = &tokens_count_in_node;
        pcount_server = &count_server;
        pcapable = &server_capable;
        pcandidate_node = &scandidate_node;
      }
      server_id_type suitable_node = make_pair(INVALID_FLAG, INVALID_FLAG);
      for(int i = CONSIDER_ALL;
          i <= CONSIDER_FORCE && suitable_node.first == INVALID_FLAG; i++) {
        int s = CONSIDER_ALL;
        for(map<int, server_list_type>::iterator it =
            pcandidate_node->begin();
            it != pcandidate_node->end() && s < i + 1
            && suitable_node.first == INVALID_FLAG; it++) {
          for(server_list_type::iterator server_it = it->second.begin();
              server_it != it->second.end(); server_it++) {
            server_id_type server_id = *server_it;
            if(is_this_node_OK
               (server_id, line_num, node_idx, hash_table_dest,
                i) == NODE_OK) {
              suitable_node = server_id;
              if(suitable_node.first == original_node.first)
                break;
            }
          }
          if(i < CONSIDER_BASE)
            s++;
        }
      }
      return suitable_node;
    }

    bool table_builder::build_quick_table(hash_table_type & hash_table_dest)
    {
      hash_table_line_type & line = hash_table_dest[0];
      for(uint32_t idx = 0; idx < bucket_count; idx++) {
        //log_debug("quick table check server %s", tbsys::CNetUtil::addrToString(line[idx].first).c_str() );
        if(is_node_availble(line[idx]) == false) {        // this will make some unbalance, but that's ok. another balance will be reache when migrating is done
          //log_debug("will change server %s",tbsys::CNetUtil::addrToString(line[idx].first).c_str());
          if(change_master_node(idx, hash_table_dest, true) == false) {
            log_error
              ("bucket %d lost all of its duplicate so can not find out a master for it quick build failed",
               idx);
            return false;
          }
        }
      }
      for(uint32_t line_number = 1; line_number < copy_count; ++line_number) {
        hash_table_line_type & line = hash_table_dest[line_number];
        for(uint32_t idx = 0; idx < bucket_count; idx++) {
          if(is_node_availble(line[idx]) == false) {
            line[idx].first = 0l;
            line[idx].second = 0;
          }
        }
      }
      return true;
    }

    //return value 0 buidl error  1 ok 2 quick build ok
    int table_builder::
      rebuild_table(const hash_table_type & hash_table_source,
                    hash_table_type & hash_table_result, bool no_quick_table)
    {
      init_token_count(tokens_count_in_node);
      init_token_count(tokens_count_in_node_now);
      init_token_count(mtokens_count_in_node);

      max_count_now = 0;

      bool need_build_quick_table = false;

      for(hash_table_type::const_iterator it = hash_table_source.begin();
          it != hash_table_source.end(); it++) {
        const hash_table_line_type & line = it->second;
        for(uint32_t i = 0; i < bucket_count; i++) {
          update_node_count(line[i], tokens_count_in_node);
          if(it->first == 0) {
            if(update_node_count(line[i], mtokens_count_in_node) == false) {
              // almost every time this will happen
              need_build_quick_table = true;
            }
          }
        }
      }

      build_index(tokens_count_in_node, count_server);
      build_index(mtokens_count_in_node, mcount_server);

      caculate_capable();

      init_candidate(mcandidate_node, &master_server_capable, &mcount_server);
      init_candidate(scandidate_node, &server_capable, &count_server);
      hash_table_result = hash_table_source;
      if(need_build_quick_table && !no_quick_table) {
        if(build_quick_table(hash_table_result)) {
          return BUILD_QUICK;
        }
        return BUILD_ERROR;
      }
      if(available_server.size() < copy_count) {
        return BUILD_ERROR;
      }
      //
      //we will check master first, then other slaves
      /////////////////////////////////////////////////////////////////////////////////////
      //checke every node and find out the bad one
      //a good one must
      //    1 the buckets this one charge of must not large than tokenPerNode ;
      //    2 the master buckets this one charge of must not large than masterTokenPerNode;
      //    3 copys of a same bucket must seperated store in different data server
      /////////////////////////////////////////////////////////////////////////////////////
      int i = 0;

      for(hash_table_type::iterator it = hash_table_result.begin();
          it != hash_table_result.end() && i < 2; it++, i++) {
        int line_num_out = it->first;
        for(uint32_t node_idx = 0; node_idx != bucket_count; node_idx++) {
          for(uint32_t line_num = line_num_out; line_num < copy_count;
              line_num++) {
            if(line_num_out == 0 && line_num != 0)
              continue;
            int change_type = 0;        //not need migrate
            server_id_type node_id = hash_table_result[line_num][node_idx];
            int consider = CONSIDER_ALL;
            if(line_num_out == 0)
              consider = CONSIDER_BASE;
            change_type =
              is_this_node_OK(node_id, line_num, node_idx, hash_table_result,
                              consider, true);
            if(change_type == INVALID_NODE) {
              node_id.first = INVALID_FLAG;
            }

            if(change_type != 0) {
#ifdef TDEBUG
              log_debug
                ("---------------------will change -------------line=%u idx=%u \n",
                 node_idx, line_num);
              log_debug("debug changeType =%d\n", change_type);
              log_debug("befor change\n");
              print_tokens_in_node();
              print_hash_table(hash_table_result);
              print_capabale();
#endif

              if(line_num == 0) {
                if(change_master_node(node_idx, hash_table_result, false)) {
                  continue;
                }
              }
              server_id_type old_node = hash_table_result[line_num][node_idx];
              invaliad_node(line_num, node_idx, hash_table_result);
              server_id_type suitable_node =
                get_suitable_node(line_num, node_idx, hash_table_result,
                                  old_node);
              if(suitable_node.first == INVALID_FLAG) {
                log_debug("I am give up, why this happend?\n");
                return BUILD_ERROR;
              }
              update_node(line_num, node_idx, suitable_node,
                          hash_table_result);
              node_id = suitable_node;
#ifdef TDEBUG
              log_debug("after change\n");
              print_tokens_in_node();
              print_hash_table(hash_table_result);
              log_debug("----------------------------------------\n");
#endif
            }
            int token_per_node_min = get_tokens_per_node(node_id);
            if(++tokens_count_in_node_now[node_id] == token_per_node_min + 1) {
              max_count_now++;
            }
          }
        }
        if(it == hash_table_result.begin()) {
          log_debug("first line ok");
        }
      }
      return BUILD_OK;
    }

    void table_builder::set_available_server(const set <
                                             node_info * >&ava_server)
    {
      available_server.clear();
      for(set<node_info *>::const_iterator it = ava_server.begin();
          it != ava_server.end(); it++) {
        available_server.
          insert(make_pair
                 ((*it)->server->server_id,
                  (*it)->server->server_id & pos_mask));
      }

    }
    void table_builder::load_hash_table(hash_table_type & hash_table_data,
                                        uint64_t * p_hash_table)
    {
      hash_table_data.clear();
      for(uint32_t i = 0; i < copy_count; i++) {
        hash_table_data[i].reserve(bucket_count);
        for(uint32_t j = 0; j < bucket_count; j++) {
          hash_table_data[i].
            push_back(make_pair(*p_hash_table, (*p_hash_table) & pos_mask));
          p_hash_table++;
        }
      }
    }
    void table_builder::
      write_hash_table(const hash_table_type & hash_table_data,
                       uint64_t * p_hash_table)
    {
      for(hash_table_type::const_iterator it = hash_table_data.begin();
          it != hash_table_data.end(); it++) {
        const hash_table_line_type & line = it->second;
        for(uint32_t i = 0; i < bucket_count; i++) {
          (*p_hash_table) = line[i].first;
          p_hash_table++;
        }
      }
    }

  }
}
