/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * put packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_PUT_PACKET_H
#define TAIR_PACKET_PUT_PACKET_H
#include <snappy.h>

#include "base_packet.hpp"
namespace tair {
   class request_put : public base_packet {
   public:
      request_put()
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = 0;
         area = 0;
         version = 0;
         expired = 0;
      }

      request_put(request_put &packet) 
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         version = packet.version;
         expired = packet.expired;
         key.clone(packet.key);
         data.clone(packet.data);
      }

      size_t size() 
      {
        return 1 + 2 + 2 + 4 + key.encoded_size() + data.encoded_size() + 16;
      }

      virtual uint16_t ns() 
      {
        return area;
      }

      ~request_put()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt16(version);
         output->writeInt32(expired);
         key.encode(output);
         data.encode_with_compress(output);
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
      {
         if (header->_dataLen < 15) {
            log_warn( "buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         area = input->readInt16();
         version = input->readInt16();
         expired = input->readInt32();

         key.decode(input);
         data.decode(input);
         key.data_meta.version = version;

         return true;
      }

   public:
      uint16_t        area;
      uint16_t        version;
      int32_t         expired;
      data_entry    key;
      data_entry    data;
   };

  class request_mput : public base_packet {
  public:
    request_mput()
    {
      setPCode(TAIR_REQ_MPUT_PACKET);
      server_flag = 0;
      area = 0;
      len = 8;
      packet_id = 0;
      reset();
    }

    request_mput(request_mput &packet)
    {
      clone(packet, packet.alloc);
    }

    ~request_mput()
    {
      clear();
    }

    void clone(request_mput &packet, bool need_alloc)
    {
      if (this == &packet) {
        return ;
      }
      clear();
      setPCode(TAIR_REQ_MPUT_PACKET);
      server_flag = packet.server_flag;
      area = packet.area;
      count = packet.count;
      len = packet.len;
      compressed = packet.compressed;
      packet_data_len = packet.packet_data_len;

      if (need_alloc) {
        if (compressed) {
          packet_data = new char[packet_data_len];
          memcpy(packet_data, packet.packet_data, packet_data_len);
        }

        alloc = true;
        record_vec = new mput_record_vec();
        mput_record_vec::iterator it;
        for (it = packet.record_vec->begin(); it != packet.record_vec->end(); ++it) {
          mput_record* rec = new mput_record(**it);
          record_vec->push_back(rec);
        }
      } else {
        alloc = packet.alloc;
        record_vec = packet.record_vec;
        packet_data = packet.packet_data;
      }

      packet_id = packet.packet_id;
    }

    // to avoid redudant memory copy, swap packet inner data owner
    void swap(request_mput &packet)
    {
      clone(packet, false);
      packet.reset();
    }

    void reset()
    {
      packet_data = NULL;
      packet_data_len = 0;
      compressed = false;

      alloc = false;
      record_vec = NULL;
      count = 0;
    }

    void clear()
    {
      if (record_vec != NULL && alloc) {
        mput_record_vec::iterator it;
        for (it = record_vec->begin(); it != record_vec->end(); ++it) {
          delete (*it);
        }

        delete record_vec;
        record_vec = NULL;
        count = 0;
      }

      if (packet_data != NULL) {
        delete packet_data;
        packet_data = NULL;
        packet_data_len = 0;
      }
    }

    bool compress()
    {
      // only compress once
      if (compressed) {
        return true;
      }

      bool ret = true;
#ifdef WITH_COMPRESS
      tbnet::DataBuffer output;
      do_encode(&output);

      int raw_len = output.getDataLen();
      // do compress, use snappy now
      ret = tair::common::compressor::do_compress
        (&packet_data, &packet_data_len,
         output.getData(), raw_len, tair::common::data_entry::compress_type) == 0 ? true : false;
      if (ret) {
        compressed = true;
      }
      //log_error("compress %d=>%d", raw_len, packet_data_len);
#endif
      return ret;
    }

    bool decompress()
    {
      if (!compressed) {
        return true;        
      }
      if (NULL == packet_data || packet_data_len <= 0) {
        return false;
      }

      bool ret = false;
#ifdef WITH_COMPRESS
      // TODO: redundant memcpy now, compressor can provide uncompresslen interface and
      //       receive malloced buffer
      tbnet::DataBuffer input;
      char* raw_data = NULL;
      int32_t raw_data_len = 0;
      ret = tair::common::compressor::do_decompress
        (&raw_data, &raw_data_len, packet_data, packet_data_len, tair::common::data_entry::compress_type) == 0 ? true : false;
      if (ret) {
        input.ensureFree(raw_data_len);
        memcpy(input.free(), raw_data, raw_data_len);
        input.pourData(raw_data_len);
        delete raw_data;
        ret = do_decode(&input);
      }
#else
      log_error("decompress data but compiled without compress support");
#endif
      return ret;
    }

    bool do_encode(tbnet::DataBuffer *output)
    {
      output->writeInt8(server_flag);
      output->writeInt16(area);
      output->writeInt32(count);
      if (record_vec != NULL) {
        mput_record_vec::iterator it;
        for (it = record_vec->begin(); it != record_vec->end(); ++it) {
          mput_record* rec = (*it);
          rec->key->encode(output);
          rec->value->encode(output);
        }
      }
      output->writeInt32(packet_id);
      return true;
    }

    bool do_decode(tbnet::DataBuffer *input)
    {
      server_flag = input->readInt8();
      area = input->readInt16();
      count = input->readInt32();

      if (count > 0) {
        record_vec = new mput_record_vec();
        alloc = true;
        for (uint32_t i = 0; i < count; i++) {
          mput_record *rec = new mput_record();
          data_entry* key = new data_entry();
          key->decode(input);
          rec->key = key;
          value_entry* value = new value_entry();
          value->decode(input);
          rec->value = value;
          record_vec->push_back(rec);
        }
      }
      packet_id = input->readInt32();
      return true;
    }

    bool encode(tbnet::DataBuffer *output)
    {
      output->writeInt8(compressed ? 1 : 0);
      if (compressed) {
        output->writeInt32(packet_data_len);
        output->writeBytes(packet_data, packet_data_len);
      } else {
        do_encode(output);
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      if (header->_dataLen < 8) {
        log_warn( "buffer data too few.");
        return false;
      }

      compressed = input->readInt8();
      if (compressed) {
        packet_data_len = input->readInt32();
        packet_data = new char[packet_data_len];
        input->readBytes(packet_data, packet_data_len);
      } else {
        do_decode(input);
      }
      return true;
    }

    bool add_put_key_data(const data_entry &key, const value_entry &data)
    {
      uint32_t temp = len + key.get_size() + 1 + data.get_size();
      if (temp > MAX_MPUT_PACKET_SIZE && count > 0) {
        log_warn("mput packet size overflow: %u", temp);
        return false;
      }
      if (record_vec == NULL) {
        record_vec = new mput_record_vec();
        alloc = true;
      }

      mput_record* rec = new mput_record();
      rec->key = new data_entry();
      rec->key->clone(key);
      rec->value = new value_entry();
      rec->value->clone(data);
      record_vec->push_back(rec);
      len += key.get_size() + 1;
      len += data.get_size();
      count++;
      return true;
    }

  public:
    uint16_t area;
    uint32_t count;
    uint32_t len;
    mput_record_vec* record_vec;
    bool alloc;
    // for compress
    bool compressed;
    char* packet_data;
    size_t packet_data_len;
    // for duplicate
    uint32_t packet_id;
  };
}
#endif
