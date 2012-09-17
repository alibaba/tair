/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for lock operations
 *
 *
 */
#ifndef TAIR_PACKET_LOCK_PACKET_H
#define TAIR_PACKET_LOCK_PACKET_H

#include "put_packet.hpp"

namespace tair {
  class request_lock : public base_packet {
  public:
    request_lock() : area(0), lock_type(LOCK_VALUE), data(NULL)
    {
      setPCode(TAIR_REQ_LOCK_PACKET);
    }
    request_lock(request_lock& packet)
    {
      setPCode(TAIR_REQ_LOCK_PACKET);
      area = packet.area;
      lock_type = packet.lock_type;
      key.clone(packet.key);
      if (PUT_AND_LOCK_VALUE == lock_type)
      {
        if (NULL == data)
        {
          data = new data_entry();
        }
        data->clone(*(packet.data));
      }
    }
    ~request_lock()
    {
      if (data != NULL)
      {
        delete data;
      }
    }
    bool encode(tbnet::DataBuffer *output)
    {
      output->writeInt16(area);
      output->writeInt32(lock_type);
      key.encode(output);
      if (PUT_AND_LOCK_VALUE == lock_type)
      {
        if (NULL == data)
        {
          return false;
        }
        data->encode_with_compress(output);
      }
      return true;
    }
    bool decode(tbnet::DataBuffer *input,tbnet::PacketHeader *header)
    {
      area = input->readInt16();
      lock_type = static_cast<LockType>(input->readInt32());
      key.decode(input);
      if (PUT_AND_LOCK_VALUE == lock_type)
      {
        if (NULL == data)
        {
          data = new data_entry();
        }
        data->decode(input);
      }
      return true;
    }

    int area;
    LockType lock_type;
    data_entry key;
    data_entry* data;
  };

} /* tair */
#endif

