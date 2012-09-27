#ifndef RETRY_ALL_PACKET_H
#define RETRY_ALL_PACKET_H
#include "base_packet.hpp"
namespace tair {
  class request_retry_all : public base_packet {
  public:
    request_retry_all() : base_packet()
    {
      setPCode(TAIR_REQ_RETRY_ALL_PACKET);
      config_version = 0;
      value = 0;
    }

    request_retry_all(request_retry_all& retry_all)
    {
      setPCode(TAIR_REQ_RETRY_ALL_PACKET);
      config_version = retry_all.config_version;
      value = retry_all.value;
    }

    ~request_retry_all()
    {
    }

    bool encode(tbnet::DataBuffer *output)
    {
      output->writeInt32(config_version);
      output->writeInt32(value);
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      if (header->_dataLen < 7) {
        log_warn("data buffer too few");
        return false;
      }
      config_version = input->readInt32();
      value = input->readInt32();
      return true;
    }

  public:
    uint32_t config_version;
    int value;
  };
}
#endif
