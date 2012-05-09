/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id: prefix_incdec_packet.hpp 392 2011-12-06 02:02:41Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PREFIX_INCDEC_PACKET_H
#define TAIR_PREFIX_INCDEC_PACKET_H
#include "base_packet.hpp"
#include "data_entry.hpp"
#include "counter_wrapper.hpp"

namespace tair {
  class request_prefix_incdec : public base_packet {
  public:
    request_prefix_incdec() {
      setPCode(TAIR_REQ_PREFIX_INCDEC_PACKET);
      server_flag = 0;
      area = 0;
      pkey = NULL;
      key_count = 0;
      key_counter_map = NULL;
    }

    request_prefix_incdec(const request_prefix_incdec &rhs) {
      setPCode(TAIR_REQ_PREFIX_INCDEC_PACKET);
      server_flag = rhs.server_flag;
      area = rhs.area;
      key_count = rhs.key_count;
      if (rhs.pkey != NULL) {
        pkey = new data_entry(*rhs.pkey);
      } else {
        pkey = NULL;
      }

      if (rhs.key_counter_map != NULL) {
        key_counter_map = new key_counter_map_t;
        key_counter_map_t::const_iterator it = rhs.key_counter_map->begin();
        while (it != rhs.key_counter_map->end()) {
          data_entry *key = new data_entry(*it->first);
          counter_wrapper *wrapper = new counter_wrapper(*it->second);
          key_counter_map->insert(make_pair(key, wrapper));
          ++it;
        }
      } else {
        key_counter_map = NULL;
      }
    }

    request_prefix_incdec& operator=(const request_prefix_incdec &rhs) {
      if (this != &rhs) {
        this->~request_prefix_incdec();
        server_flag = rhs.server_flag;
        area = rhs.area;
        key_count = rhs.key_count;
        pkey = new data_entry(*rhs.pkey);
        key_counter_map = new key_counter_map_t;
        key_counter_map_t::const_iterator it = rhs.key_counter_map->begin();
        while (it != rhs.key_counter_map->end()) {
          data_entry *key = new data_entry(*it->first);
          counter_wrapper *wrapper = new counter_wrapper(*it->second);
          key_counter_map->insert(make_pair(key, wrapper));
          ++it;
        }
      }
      return *this;
    }

    ~request_prefix_incdec() {
      if (pkey != NULL) {
        delete pkey;
        pkey = NULL;
      }
      if (key_counter_map != NULL) {
        key_counter_map_t::iterator it = key_counter_map->begin();
        while (it != key_counter_map->end()) {
          data_entry *key = it->first;
          counter_wrapper *wrapper = it->second;
          ++it;
          delete key;
          delete wrapper;
        }
        delete key_counter_map;
        key_counter_map = NULL;
      }
    }

    bool encode(tbnet::DataBuffer *output) {
      if (key_count == 0) {
        return false; //~ packet's empty
      }
      output->writeInt8(server_flag);
      output->writeInt16(area);
      pkey->encode(output);
      output->writeInt32(key_count);
      key_counter_map_t::const_iterator it = key_counter_map->begin();
      while (it != key_counter_map->end()) {
        it->first->encode(output);
        it->second->encode(output);
        ++it;
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (header->_dataLen < 7) {
        return false; //~ packet's broken
      }
      server_flag = input->readInt8();
      area = input->readInt16();

      pkey = new data_entry();
      pkey->decode(input);
      key_count = input->readInt32();
      key_counter_map = new key_counter_map_t;
      for (uint32_t i = 0; i < key_count; ++i) {
        data_entry *key = new data_entry();
        key->decode(input);
        counter_wrapper *wrapper = new counter_wrapper;
        wrapper->decode(input);
        key_counter_map->insert(make_pair(key, wrapper));
      }

      return true;
    }

    void add_key_counter(data_entry *key, counter_wrapper *wrapper, bool copy = false) {
      if (copy) {
        key = new data_entry(*key);
        wrapper = new counter_wrapper(*wrapper);
      }
      if (key_counter_map == NULL) {
        key_counter_map = new key_counter_map_t;
      }
      key_counter_map->insert(make_pair(key, wrapper));
    }

    void set_pkey(data_entry *pkey, bool copy = false) {
      if (copy) {
        pkey = new data_entry(*pkey);
      }
      if (this->pkey != NULL) {
        delete this->pkey;
      }
      this->pkey = pkey;
    }

    data_entry* get_pkey() const {
      return pkey;
    }

  public:
    typedef __gnu_cxx::hash_map<data_entry*, counter_wrapper*,
            data_entry_hash, data_entry_equal_to> key_counter_map_t;

    uint16_t      area;
    data_entry    *pkey;
    uint32_t      key_count;
    key_counter_map_t *key_counter_map;
  };

  class response_prefix_incdec : public base_packet {
  public:
    response_prefix_incdec() {
      setPCode(TAIR_RESP_PREFIX_INCDEC_PACKET);
      config_version = 0;
      code = 0;
      nsuccess = 0;
      nfailed = 0;
      success_key_value_map = NULL;
      failed_key_code_map = NULL;
    }

    response_prefix_incdec(const response_prefix_incdec &rhs) {
      setPCode(TAIR_RESP_PREFIX_INCDEC_PACKET);
      copy(rhs);
    }

    response_prefix_incdec& operator=(const response_prefix_incdec &rhs) {
      if (this != &rhs) {
        this->~response_prefix_incdec();
        copy(rhs);
      }
      return *this;
    }

    ~response_prefix_incdec() {
      key_code_map_t::iterator it;

      if (success_key_value_map != NULL) {
        it = success_key_value_map->begin();
        while (it != success_key_value_map->end()) {
          data_entry *key = it->first;
          ++it;
          delete key;
        }
        delete success_key_value_map;
        success_key_value_map = NULL;
      }

      if (failed_key_code_map != NULL) {
        it = failed_key_code_map->end();
        while (it != failed_key_code_map->end()) {
          data_entry *key = it->first;
          ++it;
          delete key;
        }
        delete failed_key_code_map;
        failed_key_code_map = NULL;
      }
    }

    bool encode(tbnet::DataBuffer *output) {
      output->writeInt32(config_version);
      output->writeInt32(code);
      output->writeInt32(nsuccess);
      key_code_map_t::iterator it;
      if (nsuccess > 0) {
        assert(success_key_value_map != NULL);
        it = success_key_value_map->begin();
        while (it != success_key_value_map->end()) {
          it->first->encode(output);
          output->writeInt32(it->second);
          ++it;
        }
      }
      output->writeInt32(nfailed);
      if (nfailed > 0) {
        assert(failed_key_code_map != NULL);
        it = failed_key_code_map->begin();
        while (it != failed_key_code_map->end()) {
          it->first->encode(output);
          output->writeInt32(it->second);
          ++it;
        }
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (header->_dataLen < 16) {
        return false; //~ packet's broken
      }
      config_version = input->readInt32();
      code = input->readInt32();

      nsuccess = input->readInt32();
      if (nsuccess > 0) {
        success_key_value_map = new key_code_map_t;
        for (uint32_t i = 0; i < nsuccess; ++i) {
          data_entry *key = new data_entry();
          key->decode(input);
          int value = input->readInt32();
          success_key_value_map->insert(make_pair(key, value));
        }
      }

      nfailed = input->readInt32();
      if (nfailed > 0) {
        failed_key_code_map = new key_code_map_t;
        for (uint32_t i = 0; i < nfailed; ++i) {
          data_entry *key = new data_entry();
          key->decode(input);
          int code = input->readInt32();
          failed_key_code_map->insert(make_pair(key, code));
        }
      }

      return true;
    }

    void add_key_value(data_entry *key, int32_t value, bool copy = false) {
      if (copy) {
        key = new data_entry(*key);
      }
      if (success_key_value_map == NULL) {
        success_key_value_map = new key_code_map_t;
      }
      success_key_value_map->insert(make_pair(key, value));
      ++nsuccess;
    }

    void add_key_code(data_entry *key, int32_t code, bool copy = false) {
      if (copy) {
        key = new data_entry(*key);
      }
      if (failed_key_code_map == NULL) {
        failed_key_code_map = new key_code_map_t;
      }
      failed_key_code_map->insert(make_pair(key, code));
      ++nfailed;
    }

    void set_code(int code) {
      this->code = code;
    }

  private:
    void copy(const response_prefix_incdec &rhs) {
      config_version = rhs.config_version;
      code = rhs.code;
      nsuccess = rhs.nsuccess;
      nfailed = rhs.nfailed;

      key_code_map_t::const_iterator it;
      if (rhs.success_key_value_map != NULL) {
        success_key_value_map = new key_code_map_t;
        it = rhs.success_key_value_map->begin();
        while (it != rhs.success_key_value_map->end()) {
          data_entry *key = new data_entry(*it->first);
          success_key_value_map->insert(make_pair(key, it->second));
          ++it;
        }
      } else {
        success_key_value_map = NULL;
      }

      if (rhs.failed_key_code_map != NULL) {
        failed_key_code_map = new key_code_map_t;
        it = rhs.failed_key_code_map->begin();
        while (it != rhs.failed_key_code_map->end()) {
          data_entry *key = new data_entry(*it->first);
          failed_key_code_map->insert(make_pair(key, it->second));
          ++it;
        }
      } else {
        failed_key_code_map = NULL;
      }
    }

  public:
    typedef __gnu_cxx::hash_map<data_entry*, int32_t,
                      data_entry_hash, data_entry_equal_to> key_code_map_t;

    uint32_t          config_version;
    int32_t           code;
    uint32_t          nsuccess;
    uint32_t          nfailed;
    key_code_map_t    *success_key_value_map;
    key_code_map_t    *failed_key_code_map;
  };
}

#endif
