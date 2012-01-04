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
#ifndef TABLE_BUILDER_H
#define TABLE_BUILDER_H
#include <vector>
#include <string>
#include <map>
#include <set>
#include <utility>
#include <algorithm>
#include "define.hpp"
#include "server_info.hpp"

namespace tair {
  namespace config_server {
    using namespace std;

    const int CONSIDER_ALL = 0;
    const int CONSIDER_POS = 1;
    const int CONSIDER_BASE = 2;
    const int CONSIDER_FORCE = 3;

    const int NODE_OK = 0;
    const int INVALID_NODE = 1;
    const int TOOMANY_MASTER = 2;
    const int TOOMANY_BUCKET = 3;
    const int SAME_NODE = 4;
    const int SAME_POS = 5;

    const uint64_t INVALID_FLAG = 0;

    const int BUILD_ERROR = 0;
    const int BUILD_OK = 1;
    const int BUILD_QUICK = 2;

    enum DataLostToleranceFlag {
      NO_DATA_LOST_FLAG = 0,
      ALLOW_DATA_LOST_FALG
    };

    class table_builder {
      public:
        table_builder(uint32_t bucket_c,
            uint32_t copy_c):bucket_count(bucket_c),
        copy_count(copy_c)
      {
        pos_mask = TAIR_POS_MASK;
        d_lost_flag = NO_DATA_LOST_FLAG;
      }
        virtual ~ table_builder();
        typedef pair<uint64_t, uint32_t> server_id_type;

        typedef vector<server_id_type> hash_table_line_type;

        // map<copy_id, vector<server_id, pos_mask_id> >  : vector's sequence number means bucket number
        typedef map<int, hash_table_line_type> hash_table_type;
        typedef set<server_id_type> server_list_type;
        typedef map<server_id_type, int>server_capable_type;
        void load_hash_table(hash_table_type & hash_table,
            uint64_t * p_hash_table);
        void write_hash_table(const hash_table_type & hash_table,
            uint64_t * p_hash_table);
        bool build_quick_table(hash_table_type & hash_table_dest);

        void set_pos_mask(uint64_t m)
        {
          pos_mask = m;
        }

        void set_data_lost_flag(const DataLostToleranceFlag data_lost_flag)
        {
          d_lost_flag = data_lost_flag;
        }
      public:
        virtual int rebuild_table(const hash_table_type & hash_table_source,
            hash_table_type & hash_table_dest,
            bool no_quick_table = false);
        virtual void set_available_server(const set <node_info *>&ava_ser);
        virtual int is_this_node_OK(server_id_type node_id, int line_num,
            size_t node_idx,
            hash_table_type & hash_table_dest,
            int option_level, bool node_in_use =
            false) = 0;
        virtual void caculate_capable() = 0;
        virtual int get_tokens_per_node(const server_id_type &) = 0;

      public:
        /////////////////////////////////////////////////////////////////////////////////////////////////////////
        //这些打印函数是为了调试使用的
        void print_tokens_in_node();
        void print_count_server();
        void print_available_server();
        void print_hash_table(hash_table_type & hash_table);
        void print_capabale();

        /////////////////////////////////////////////////////////////////////////////////////////////////////////
      private:
        table_builder & operator =(const table_builder &);
        table_builder(const table_builder &);

      protected:
        void init_token_count(map<server_id_type, int>&collector);
        bool update_node_count(server_id_type node_id, map<server_id_type,int>&collector);
        void build_index(const map<server_id_type, int>&collector, map<int, server_list_type> &indexer);

        bool is_node_availble(server_id_type node_id);


        void invaliad_node(int line_num, size_t node_idx,
            hash_table_type & hash_table);
        bool change_master_node(size_t idx, hash_table_type & hash_table_dest,
            bool force_flag = false);
        void update_node(int line_num, size_t node_idx,
            const server_id_type & suitable_node,
            hash_table_type & hash_table_dest);
        server_id_type get_suitable_node(int line_num, size_t node_idx,
            hash_table_type & hash_table_dest,
            server_id_type &);

        void init_candidate(map<int, server_list_type> &candidate_node,
            server_capable_type *p_capable, map<int, server_list_type> *p_count_server);
        static void change_tokens_count_in_node(map<server_id_type, int>&count_in_node,
            const server_id_type & node_id,
            map<int, server_list_type> &count_server,
            map <int, server_list_type> &candidate_node,
            server_capable_type & server_capable,
            bool minus = true);
      protected:
        map <server_id_type, int> tokens_count_in_node;
        int max_count_now;

        // count of buckets the server hold
        map<server_id_type, int> tokens_count_in_node_now;
        map<int, server_list_type> count_server;

        // count of master buckets the server hold
        map<server_id_type, int> mtokens_count_in_node;
        // map<master bucket count, server>
        map<int, server_list_type> mcount_server;

        // map<(now_count - capable_count), server>
        map<int, server_list_type> scandidate_node;
        map<int, server_list_type> mcandidate_node;

        server_list_type available_server;
        server_capable_type server_capable;
        server_capable_type master_server_capable;

        uint64_t pos_mask;
        uint32_t bucket_count;
        uint32_t copy_count;

        DataLostToleranceFlag d_lost_flag;
    };
  }
}
#endif
