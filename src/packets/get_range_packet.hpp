/*
 * (C) 2011-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */

#ifndef TAIR_PACKET_GET_RANGE_PACKET_H
#define TAIR_PACKET_GET_RANGE_PACKET_H
#include "base_packet.hpp"

namespace tair {
  class request_get_range : public base_packet {
    public:
      request_get_range()
      {
        setPCode(TAIR_REQ_GET_RANGE_PACKET);
        cmd = 0;
        server_flag = 0;
        area = 0;
        offset = 0;
        limit = 0;
      }

      request_get_range(request_get_range &packet)
      {
        setPCode(TAIR_REQ_GET_RANGE_PACKET);
        cmd = packet.cmd;
        server_flag = packet.server_flag;
        area = packet.area;
        offset = packet.offset;
        limit = packet.limit;
        key_start = packet.key_start;
        key_end = packet.key_end;
      }

      ~request_get_range()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
        output->writeInt8(server_flag);
        output->writeInt16(cmd);
        output->writeInt16(area);
        output->writeInt32(offset);
        output->writeInt32(limit);

        key_start.encode(output);
        key_end.encode(output);
        return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
        if (header->_dataLen < 7) {
          log_warn( "buffer data too few.");
          return false;
        }
        server_flag = input->readInt8();
        cmd = input->readInt16();
        area = input->readInt16();
        offset = input->readInt32();     
        limit = input->readInt32();     

        key_start.decode(input);
        key_end.decode(input);

        return true;
      }

    public:
      uint16_t cmd;
      uint16_t area;
      uint32_t offset;
      uint32_t limit;
      data_entry key_start;
      data_entry key_end;
  };

  class response_get_range : public base_packet {
    public:

      response_get_range()
      {
        config_version = 0;
        setPCode(TAIR_RESP_GET_RANGE_PACKET);
        key_count = 0;
        key_data_vector = NULL;
        proxyed_key_list = NULL;
        code = 0;
        cmd = 0;
        flag = 0;
      }

      ~response_get_range()
      {
        if (key_data_vector != NULL) {
          for (uint32_t i=0; i<key_data_vector->size(); i++) {
            if ((*key_data_vector)[i] != NULL)
              delete (*key_data_vector)[i];
          }
          delete key_data_vector;
        }
      }


      bool encode(tbnet::DataBuffer *output)
      {
        output->writeInt32(config_version);
        output->writeInt32(code);
        output->writeInt16(cmd);
        output->writeInt32(key_count);
        output->writeInt16(flag);
        if (key_data_vector != NULL) {
          for (uint32_t i=0; i<key_data_vector->size(); i++) {
            data_entry *data = (*key_data_vector)[i];
            if ( data != NULL)
              data->encode(output);
          }
          output->writeInt32(0);
        }
        return true;
      }

      void set_cmd(int cmd)
      {
        this->cmd = cmd;
      }

      void set_code(int code)
      {
        this->code = code;
      }

      int get_code()
      {
        return code;
      }

      void set_hasnext(bool hasnext)
      {
        if (hasnext)
            flag |= FLAG_HASNEXT;
        else 
            flag &= FLAG_HASNEXT_MASK;
      }

      bool get_hasnext()
      {
        return flag & FLAG_HASNEXT;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
        if (header->_dataLen < 8) 
        {
          log_warn( "buffer data too few.");
          return false;
        }
        config_version = input->readInt32();
        code = input->readInt32();
        cmd = input->readInt16();
        key_count = input->readInt32();
        flag = input->readInt16();

        key_data_vector = new tair_dataentry_vector();
        for (uint32_t i=0; i<key_count; i++) 
        {
          data_entry *key = new data_entry();
          key->decode(input);
          key_data_vector->push_back(key);
        }
        if (key_count > 0)
        {
          input->readInt32();
        }

        return true;
      }

      // add key and data
      void add_key_data(data_entry *key, data_entry *value)
      {
        if (key_data_vector == NULL) {
          key_data_vector = new tair_dataentry_vector;
        }
        key_data_vector->push_back(key);
        key_data_vector->push_back(value);
        key_count ++;
      }

      void set_key_data_vector(tair_dataentry_vector *result)
      {
        if (key_data_vector != NULL) 
        {
          for (uint32_t i = 0; i < key_data_vector->size(); i++) 
          {
            if ((*key_data_vector)[i] != NULL)
            {
              delete (*key_data_vector)[i];
            }
          }
          delete key_data_vector;
        }

        key_data_vector = result;
      }

      void set_key_count(int count)
      {
        key_count = count; 
      }

    public:
      uint32_t config_version;
      uint32_t key_count;
      uint16_t cmd;
      uint16_t flag;
      tair_dataentry_vector *key_data_vector;
      tair_dataentry_set  *proxyed_key_list;
    private:
      int code;
      response_get_range(const response_get_range&);
  };
}
#endif //TAIR_PACKET_GET_RANGE_PACKET_H

