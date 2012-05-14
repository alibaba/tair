/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: prefix_puts_packet.hpp 392 2012-03-13 02:02:41Z $
 *
 * Authors:
 *   		<ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PREFIX_PUTS_PACKET_H
#define TAIR_PREFIX_PUTS_PACKET_H

#include "base_packet.hpp"

namespace tair {
  class request_prefix_puts : public base_packet {
  public:
    request_prefix_puts() {
      setPCode(TAIR_REQ_PREFIX_PUTS_PACKET);
      area = 0;
      key_count = 0;
      pkey = NULL;
      kvmap = NULL;
    }

    request_prefix_puts(const request_prefix_puts &packet) {
      setPCode(TAIR_REQ_PREFIX_PUTS_PACKET);
      area = packet.area;
      key_count = packet.key_count;
      pkey = new data_entry(*packet.pkey);
      kvmap = new tair_keyvalue_map();
      tair_keyvalue_map::const_iterator it = packet.kvmap->begin();
      while (it != packet.kvmap->end()) {
        data_entry *key = new data_entry(*it->first);
        data_entry *value = new data_entry(*it->second);
        kvmap->insert(make_pair(key, value));
        ++it;
      }
    }

    ~request_prefix_puts() {
      if (pkey == NULL) {
        delete pkey;
        pkey = NULL;
      }
      if (kvmap != NULL) {
        tair_keyvalue_map::iterator it = kvmap->begin();
        while (it != kvmap->end()) {
          data_entry *key = it->first;
          data_entry *value = it->second;
          ++it;
          delete key;
          delete value;
        }
        delete kvmap;
        kvmap = NULL;
      }
    }

    bool encode(tbnet::DataBuffer *output) {
      log_error("not supported now");
      return false;
      //if (pkey == NULL || kvmap == NULL) {
      //log_error("packet not complete");
      //return false;
      //}
      //output->writeInt8(server_flag);
      //output->writeInt16(area);
      //output->writeInt16(version);
      //output->writeInt32(expired);

      //pkey->encode(output);

      //output->writeInt32(key_count);
      //tair_keyvalue_map::iterator it = kvmap->begin();
      //while (it != kvmap->end()) {
      //it->first->encode(output);
      //it->second->encode(output);
      //++it;
      //}
      //return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (header->_dataLen < 11) {
        log_warn("buffer data too few");
        return false;
      }
      server_flag = input->readInt8();
      area = input->readInt16();

      pkey = new data_entry();
      pkey->decode(input);

      key_count = input->readInt32();
      kvmap = new tair_keyvalue_map();
      for (uint32_t i = 0; i < key_count; ++i) {
        data_entry *key = new data_entry();
        data_entry *value = new data_entry();
        key->decode(input);
        value->decode(input);
        if (kvmap->insert(make_pair(key, value)).second == false) {
          delete key;
          delete value;
        }
      }
      key_count = kvmap->size();

      return true;
    }

    void set_pkey(char *key, int size) {
      if (pkey != NULL) {
        delete pkey;
      }
      pkey = new data_entry(key, size);
    }

    void set_pkey(data_entry *key) {
      if (pkey != NULL) {
        delete pkey;
      }
      pkey = key;
    }

    void add_key_value(data_entry *key, data_entry *value) {
      if (kvmap == NULL) {
        kvmap = new tair_keyvalue_map();
      }
      kvmap->insert(make_pair(key, value));
      ++key_count;
    }

  public:
    uint16_t area;
    uint32_t key_count;
    data_entry *pkey;
    tair_keyvalue_map *kvmap;
  private:
    request_prefix_puts& operator=(const request_prefix_puts&);
  };
}

#endif
