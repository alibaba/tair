/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * manager for route table pull from configserver impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include <algorithm>
#include "table_manager.hpp"
#include "log.hpp"
#include "scoped_wrlock.hpp"

namespace {
   const int MISECONDS_ASSURANCE_TIME = 30;
}
namespace tair {

  using namespace std;

   table_manager::table_manager()
   {
      server_table = NULL;
      table_version = 0u;
      copy_count = 0u;
      bucket_count = 0u;
     pthread_rwlock_init(&m_mutex, 0);
   }

   table_manager::~table_manager()
   {
      if (server_table != NULL) {
         delete [] server_table;
         server_table = NULL;
      }
     pthread_rwlock_destroy(&m_mutex);
   }

   bool table_manager::is_master(int bucket_number, int server_flag) 
   {
      tair::common::CScopedRwLock __scoped_lock(&m_mutex,false);
      assert (server_table != NULL);
      int index = bucket_number;
      if (server_flag == TAIR_SERVERFLAG_PROXY || server_flag == TAIR_SERVERFLAG_RSYNC_PROXY)
         index += get_hash_table_size();
      return server_table[index] == local_server_ip::ip;
   }

   void table_manager::do_update_table(uint64_t *new_table, size_t size, uint32_t table_version, uint32_t copy_count, uint32_t bucket_count)
   {
      this->copy_count = copy_count;
      this->bucket_count = bucket_count;
      assert((size % get_hash_table_size()) == 0);
#ifdef TAIR_DEBUG
      log_debug("list a new table size = %d ", size);
      for (size_t i = 0; i < size; ++i) {
         log_debug("serverTable[%d] = %s", i, tbsys::CNetUtil::addrToString(new_table[i]).c_str() );
      }
#endif

      uint64_t *temp_table = new uint64_t[size];
      memcpy(temp_table, new_table, size * sizeof(uint64_t));

      tair::common::CScopedRwLock __scoped_lock(&m_mutex,true);
      uint64_t *old_table = server_table;
      server_table = temp_table;
      if (old_table != NULL) {
         usleep(MISECONDS_ASSURANCE_TIME);
         delete [] old_table;
      }

      // clear the containers
      padding_buckets.clear();
      release_buckets.clear();
      migrates.clear();
      available_server_ids.clear();

      vector<int> temp_holding_buckets;

      for (size_t i=0; i<get_hash_table_size(); i++) {
         available_server_ids.insert(new_table[i]);
         if (new_table[i] == local_server_ip::ip) {
            log_debug("take bucket: %d", (i%this->bucket_count));
            temp_holding_buckets.push_back(i % this->bucket_count);

            if (i < this->bucket_count && size > get_hash_table_size()) {
               calculate_migrates(new_table, i);

            }
         }
      }
#ifdef TAIR_DEBUG
      log_debug("caculate migrate ok size = %d", migrates.size());
      bucket_server_map_it it = migrates.begin();
      for (; it != migrates.end() ; ++it) {
         for(vector<uint64_t>::iterator it2 = it->second.begin(); it2 != it->second.end(); it2++) {
            log_debug("bucket id:%d will migrate to server id %s ", it->first,tbsys::CNetUtil::addrToString(*it2).c_str());
         }
      }
#endif

      if (size > get_hash_table_size()) {
         // have migrate table
         for (size_t i=get_hash_table_size(); i<2*get_hash_table_size(); i++) {
            available_server_ids.insert(new_table[i]);
            if (new_table[i] == local_server_ip::ip)
               padding_buckets.push_back(i % this->bucket_count);
         }
      }

      calculate_release_bucket(temp_holding_buckets);

      holding_buckets = temp_holding_buckets;
      this->table_version = table_version;
#ifdef TAIR_DEBUG
      for (size_t i=0; i<holding_buckets.size(); i++) {
         log_debug("holding bucket: %d", holding_buckets[i]);
      }
#endif
   }

   vector<uint64_t> table_manager::get_slaves(int bucket_number, bool is_migrating) 
   {
      tair::common::CScopedRwLock __scoped_lock(&m_mutex,false);
      assert (server_table != NULL);

      vector<uint64_t> slaves;

      int end = get_hash_table_size();
      int index = bucket_number + this->bucket_count;
      if (is_migrating) {
         index += get_hash_table_size();
         end += get_hash_table_size();
      }

      // make sure I am the master
      if (server_table[index - this->bucket_count] != local_server_ip::ip) {
         log_info("I am not master, have no slave.");
         return slaves;
      }

      do {
         uint64_t sid = server_table[index];
         if (sid != 0) {
            log_debug("add slave: %s", tbsys::CNetUtil::addrToString(sid).c_str());
            slaves.push_back(sid);
         }
         index += this->bucket_count;
      } while (index < end);

      return slaves;
   }

   uint64_t table_manager::get_migrate_target(int bucket_number) 
   {
      tair::common::CScopedRwLock __scoped_lock(&m_mutex,false);
      assert (server_table != NULL);

      return server_table[bucket_number + get_hash_table_size()];
   }

   void table_manager::calculate_release_bucket(vector<int> &new_holding_bucket)
   {
      // sort the vectors
      sort(new_holding_bucket.begin(), new_holding_bucket.end());
      sort(holding_buckets.begin(), holding_buckets.end());

      set_difference(holding_buckets.begin(), holding_buckets.end(),
                     new_holding_bucket.begin(), new_holding_bucket.end(),
                     inserter(release_buckets, release_buckets.end()));
   }

   void table_manager::calculate_migrates(uint64_t *table, int index)
   {
      uint64_t* psource_table = table;
      uint64_t* pdest_table = table + get_hash_table_size();
      for (size_t i = 0; i < this->copy_count; ++i) {

         bool need_migrate = true;
         uint64_t dest_dataserver = pdest_table[index + this->bucket_count * i];

         for (size_t j =0; j < this->copy_count; ++j) 
         {
            if (dest_dataserver == psource_table[index + this->bucket_count * j]) {
               need_migrate = false;
               break;
            }
         }

         if (need_migrate) {
            log_debug("add migrate item: bucket[%d] => %s", index, tbsys::CNetUtil::addrToString(dest_dataserver).c_str());
            migrates[index].push_back(dest_dataserver);
         }
      }
      bucket_server_map_it it = migrates.find(index);
      if (it == migrates.end()) 
      {
         if (psource_table[index] != pdest_table[index]) 
         {
            //this will add a empty vector to migrates,
            //some times only need to change master for a bucket
            migrates[index];
         }
      }
   }

   void table_manager::init_migrate_done_set(boost::dynamic_bitset<> &migrate_done_set, const vector<uint64_t> &current_state_table)
   {
      tair::common::CScopedRwLock __scoped_lock(&m_mutex,false);
      int bucket_number = 0;
      for (size_t i=0; i< current_state_table.size(); i+= this->copy_count) {
         bucket_number = (int)current_state_table[i++]; // skip the bucket_number
         bool has_migrated = false;
         for (size_t j=0; j < this->copy_count; ++j) {
            if (current_state_table[i+j] != server_table[bucket_number + j * this->bucket_count]) {
               has_migrated = true;
               break;
            }
         }
         if (has_migrated) {
            migrate_done_set.set(bucket_number, true);
            log_debug("bucket[%d] has migrated");
         }
      }
   }

   /**
   const uint64_t* table_manager::get_server_table() const
   {
      assert (server_table != NULL);
      return server_table;
   }
   **/

   vector<int> table_manager::get_holding_buckets() const
   {
      return holding_buckets;
   }

   vector<int> table_manager::get_padding_buckets() const
   {
      return padding_buckets;
   }

   vector<int> table_manager::get_release_buckets() const
   {
      return release_buckets;
   }

   bucket_server_map table_manager::get_migrates() const
   {
      return migrates;
   }

   set<uint64_t> table_manager::get_available_server_ids() const
   {
      return available_server_ids;
   }

   void table_manager::clear_available_server()
   {
      available_server_ids.clear();
   }

   void table_manager::set_table_for_localmode()
   {
      copy_count = 1u;
      bucket_count = 1u;
   }

   uint32_t table_manager::get_version() const
   {
      return table_version;
   }
   uint32_t table_manager::get_copy_count() const
   {
      return copy_count;
   }
   uint32_t table_manager::get_bucket_count() const
   {
      return bucket_count;
   }

   uint32_t table_manager::get_hash_table_size() const
   {
      return bucket_count * copy_count;
   }

}
