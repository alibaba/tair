/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * data structure used for stat
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_STATINFO_H
#define TAIR_STATINFO_H
#include <tbsys.h>
#include <tbnet.h>
namespace tair {
#pragma pack(1)
   class tair_stat {
   public:
      tair_stat() {
         atomic_set(&get_count_value, 0);
         atomic_set(&put_count_value, 0);
         atomic_set(&remove_count_value, 0);
         atomic_set(&evict_count_value, 0);
         atomic_set(&hit_count_value, 0);
         data_size_value = 0;
         use_size_value = 0;
         item_count_value = 0;
      }

      int get_count() { return atomic_read(&get_count_value); }
      void inc_get_count() { atomic_inc(&get_count_value); }
      void set_get_count(int new_value) { atomic_set(&get_count_value, new_value); }

      int put_count() { return atomic_read(&put_count_value); }
      void inc_put_count() { atomic_inc(&put_count_value); }
      void add_put_count(int count) { atomic_add(count, &put_count_value); }
      void set_put_count(int new_value) { atomic_set(&put_count_value, new_value); }

      int evict_count() { return atomic_read(&evict_count_value); }
      void inc_evict_count() { atomic_inc(&evict_count_value); }
      void set_evict_count(int new_value) { atomic_set(&evict_count_value, new_value); }

      int remove_count() { return atomic_read(&remove_count_value); }
      void inc_remove_count() { atomic_inc(&remove_count_value); }
      void set_remove_count(int newValue) { atomic_set(&remove_count_value, newValue); }

      int hit_count() { return atomic_read(&hit_count_value); }
      void inc_hit_count() { atomic_inc(&hit_count_value); }
      void set_hit_count(int newValue) { atomic_set(&hit_count_value, newValue); }

      uint64_t data_size() { return data_size_value; }
      uint64_t use_size() { return use_size_value; }
      uint64_t item_count() { return item_count_value; }

   private:
      atomic_t get_count_value;
      atomic_t put_count_value;
      atomic_t evict_count_value;
      atomic_t remove_count_value;
      atomic_t hit_count_value;

   public:
      uint64_t data_size_value;
      uint64_t use_size_value;
      uint64_t item_count_value;
   };
   class stat_helper;
   class tair_pstat {
   public:
     tair_pstat() { reset(); }
     void reset()
     {
        data_size_value = 0;
        use_size_value = 0;
        item_count_value = 0;
     }

      uint64_t data_size() { return data_size_value; }
      void add_data_size(uint64_t ds) { data_size_value += ds; }
      void sub_data_size(uint64_t ds) { data_size_value -= ds; }

      uint64_t use_size() { return use_size_value; }
      void add_use_size(uint64_t ds) { use_size_value += ds; }
      void sub_use_size(uint64_t ds) { use_size_value -= ds; }

      uint64_t item_count() { return item_count_value; }
      void add_item_count(int c=1) { item_count_value += c; }
      void sub_item_count(int c=1) { item_count_value -= c; }
      friend class stat_helper;

   private:
      uint64_t data_size_value;
      uint64_t use_size_value;
      uint64_t item_count_value;
   };
#pragma pack()

}
#endif
