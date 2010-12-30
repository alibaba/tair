/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this dump manager is used when we ask the data servers to dump data for us
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "dump_manager.hpp"

namespace tair {
  namespace storage {
    using namespace std;
    dump_thread::dump_thread(const set<dump_filter> &dumpFilter,
                             const set <uint32_t> &buckets, uint32_t now,
                             storage_manager * p_storage_manager): cancled(false), running(true), buckets(buckets),
      storage_mgr(p_storage_manager)
    {
      set <dump_filter>::const_iterator it;
      for(it = dumpFilter.begin(); it != dumpFilter.end(); it++)
      {
        dump_filter tmp(*it);
          tmp.turn_interval(now);
          dump_filters.push_back(tmp);
      }
    }
    void dump_thread::run(tbsys::CThread * thread, void *arg)
    {
      set <uint32_t>::iterator it;
      vector <dump_filter>::iterator it_filter;
      md_info info;
      for(it = buckets.begin(); it != buckets.end() && !cancled; it++) {
        info.db_id = *it;
        info.is_migrate = false;
        storage_mgr->begin_scan(info);
        bool isHaveItem = true;
        vector<item_data_info *>items;
        while(!cancled && isHaveItem) {
          isHaveItem = storage_mgr->get_next_items(info, items);
          for(vector<item_data_info *>::iterator itor = items.begin();
              itor != items.end() && !cancled; itor++) {
            item_data_info *item = *itor;
            data_entry key(item->m_data, item->header.keysize, false);
            key.has_merged = true;
            uint32_t area = (uint32_t) key.decode_area();
            key.data_meta = item->header;
            data_entry value(item->m_data + item->header.keysize,
                             item->header.valsize, false);
            for(it_filter = dump_filters.begin();
                it_filter != dump_filters.end() && !cancled; it_filter++) {
              if(!it_filter->do_dump(key, value, area)) {
                cancled = true;
              }
            }
            free(*itor);
          }
          items.clear();
        }
        storage_mgr->end_scan(info);
      }
      //done
      for(it_filter = dump_filters.begin(); it_filter != dump_filters.end();
          it_filter++) {
        it_filter->end_dump(cancled);
      }
      running = false;
    }
    bool dump_thread::is_alive() const
    {
      return running;
    }
    void dump_thread::cancle()
    {
      cancled = true;
    }
  dump_manager::dump_manager(storage_manager * p):storage_mgr(p) {
    }
    void dump_manager::do_dump(const std::set <dump_filter> &dumpFilter,
                               const set<uint32_t> &bucket, uint32_t now)
    {
      dump_thread *pthread =
        new dump_thread(dumpFilter, bucket, now, storage_mgr);
      pthread->start();
      tbsys::CThreadGuard guard(&mutex);
      release_reference();
      set_thread.insert(pthread);

    }
    void dump_manager::cancle_all()
    {
      tbsys::CThreadGuard guard(&mutex);
      release_reference();
      set<dump_thread *>::iterator it = set_thread.begin();
      for(; it != set_thread.end(); it++) {
        (*it)->cancle();
      }
    }
    bool dump_manager::is_all_stoped()
    {
      tbsys::CThreadGuard guard(&mutex);
      release_reference();
      return set_thread.empty();
    }
    dump_manager::~dump_manager() {
      while(!set_thread.empty()) {
        cancle_all();
        release_reference();
        usleep(5000);
      }
    }
    void dump_manager::release_reference()
    {
      std::set <dump_thread *>::iterator it = set_thread.begin();
      while(it != set_thread.end()) {
        if(!(*it)->is_alive()) {
          (*it)->wait();
          delete(*it);
          set_thread.erase(it++);
        }
        else {
          it++;
        }
      }
    }

  }

}
