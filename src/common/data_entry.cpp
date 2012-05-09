/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * data_entry wrap item of tair, it can be key or value
 *
 * Version: $Id: data_entry.hpp 737 2012-04-17 08:58:19Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "data_entry.hpp"

namespace tair
{
  namespace common {
    void defree(tair_dataentry_vector &vector) {
      for (size_t i = 0; i < vector.size(); ++i) {
        delete vector[i];
      }
    }

    void defree(tair_dataentry_set &set) {
      tair_dataentry_set::iterator itr = set.begin();
      while (itr != set.end()) {
        delete *itr++;
      }
    }

    void defree(tair_keyvalue_map &map) {
      tair_keyvalue_map::iterator itr = map.begin();
      while (itr != map.end()) {
        data_entry *key = itr->first;
        data_entry *value = itr->second;
        ++itr;
        delete key;
        delete value;
      }
    }

    void defree(key_code_map_t &map) {
      key_code_map_t::iterator itr = map.begin();
      while (itr != map.end()) {
        data_entry *key = itr->first;
        ++itr;
        delete key;
      }
    }

    void merge_key(const data_entry &pkey, const data_entry &skey, data_entry &mkey) {
      int pkey_size = pkey.get_size();
      int skey_size = skey.get_size();
      char *buf = new char[pkey_size + skey_size];
      memcpy(buf, pkey.get_data(), pkey_size);
      memcpy(buf + pkey_size, skey.get_data(), skey_size);
      mkey.set_alloced_data(buf, pkey_size + skey_size);
      mkey.set_prefix_size(pkey_size);
    }

    void split_key(const data_entry *mkey, data_entry *pkey, data_entry *skey) {
      if (mkey == NULL)
        return ;
      int prefix_size = mkey->get_prefix_size();
      if (pkey != NULL) {
        pkey->set_data(mkey->get_data(), prefix_size);
      }
      if (skey != NULL) {
        skey->set_data(mkey->get_data() + prefix_size, mkey->get_size() - prefix_size);
      }
    }
  }
}
