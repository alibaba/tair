/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * get packet
 *
 * Version: $Id: prefix_gets_packet.hpp 392 2011-12-06 02:02:41Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PREFIX_GETS_PACKET_HPP
#define TAIR_PREFIX_GETS_PACKET_HPP
#include "get_packet.hpp"

namespace tair {
  class request_prefix_gets : public request_get {
  public:
    request_prefix_gets() : request_get() {
      setPCode(TAIR_REQ_PREFIX_GETS_PACKET);
    }
    request_prefix_gets(request_prefix_gets &rhs) : request_get(rhs) {
      setPCode(TAIR_REQ_PREFIX_GETS_PACKET);
    }
  };

  class response_prefix_gets : public base_packet {
  public:
    response_prefix_gets() {
      setPCode(TAIR_RESP_PREFIX_GETS_PACKET);
      config_version = 0;
      code = 0;
      nsuccess = 0;
      nfailed = 0;
      pkey = NULL;
      key_value_map = NULL;
      extra_code_map = NULL;
      key_code_map = NULL;
    }

    ~response_prefix_gets() {
      if (pkey != NULL) {
        delete pkey;
      }
      if (key_value_map != NULL) {
        defree(*key_value_map);
        delete key_value_map;
        key_value_map = NULL;
      }
      if (extra_code_map != NULL) {
        delete extra_code_map;
        extra_code_map = NULL;
      }
      if (key_code_map != NULL) {
        defree(*key_code_map);
        delete key_code_map;
        key_code_map = NULL;
      }
    }

    bool encode(tbnet::DataBuffer *output) {
      output->writeInt32(config_version);
      output->writeInt32(code);
      pkey->encode(output);

      output->writeInt32(nsuccess);
      if (nsuccess > 0) {
        tair_keyvalue_map::const_iterator it = key_value_map->begin();
        while (it != key_value_map->end()) {
          it->first->encode(output);
          it->second->encode(output);
          output->writeInt32(extra_code_map->operator[](it->first));
          ++it;
        }
      }
      output->writeInt32(nfailed);
      if (nfailed > 0) {
        key_code_map_t::const_iterator it = key_code_map->begin();
        while (it != key_code_map->end()) {
          it->first->encode(output);
          output->writeInt32(it->second);
          ++it;
        }
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (header->_dataLen < 17) {
        return false;
      }
      config_version = input->readInt32();
      code = input->readInt32();
      pkey = new data_entry();
      pkey->decode(input);
      nsuccess = input->readInt32();
      if (nsuccess > 0) {
        key_value_map = new tair_keyvalue_map;
        for (uint32_t i = 0; i < nsuccess; ++i) {
          data_entry *key = new data_entry;
          key->decode(input);
          data_entry *value = new data_entry;
          value->decode(input);
          extra_code_map->insert(make_pair(key, input->readInt32()));
          key_value_map->insert(make_pair(key, value));
        }
      }

      nfailed = input->readInt32();
      if(nfailed > 0) {
        key_code_map = new key_code_map_t;
        for (uint32_t i = 0; i < nfailed; ++i) {
          data_entry *key = new data_entry;
          key->decode(input);
          int rc = input->readInt32();
          key_code_map->insert(make_pair(key, rc));
        }
      }
      return true;
    }

    void set_code(int code) {
      this->code = code;
    }

    int get_code() const {
      return this->code;
    }

    void set_config_version(uint32_t config_version) {
      this->config_version = config_version;
    }

    void set_pkey(data_entry *pkey, bool copy = false) {
      if (copy) {
        pkey = new data_entry(*pkey);
      }
      if (this->pkey != NULL) {
        delete this->pkey;
        this->pkey = NULL;
      }
      this->pkey = pkey;
    }

    void set_pkey(const char *data, uint32_t size) {
      if (pkey == NULL) {
        pkey = new data_entry();
      }
      pkey->set_data(data, size);
    }

    void add_key_value(data_entry *key, data_entry *value, bool copy = false, int code = 0) {
      if (key == NULL || value == NULL) {
        return ;
      }
      if (copy) {
        key = new data_entry(*key);
        value = new data_entry(*value);
      }
      if (key_value_map == NULL) {
        key_value_map = new tair_keyvalue_map;
      }
      if (extra_code_map == NULL) {
        extra_code_map = new key_code_map_t;
      }
      key_value_map->insert(make_pair(key, value));
      extra_code_map->insert(make_pair(key, code));
      ++nsuccess;
    }

    void add_key_code(data_entry *key, int code, bool copy = false) {
      if (key == NULL) {
        return ;
      }
      if (copy) {
        key = new data_entry(*key);
      }
      if (key_code_map == NULL) {
        key_code_map = new key_code_map_t;
      }
      key_code_map->insert(make_pair(key, code));
      ++nfailed;
    }

  public:
    typedef __gnu_cxx::hash_map<data_entry*, int,
            data_entry_hash, data_entry_equal_to> key_code_map_t;
    uint32_t config_version;
    int code;
    uint32_t nsuccess;
    uint32_t nfailed;
    data_entry *pkey;
    tair_keyvalue_map *key_value_map;
    key_code_map_t *extra_code_map;
    key_code_map_t *key_code_map;
  private:
    response_prefix_gets(const response_prefix_gets&);
    response_prefix_gets& operator=(const response_prefix_gets&);
  };
}

#endif
