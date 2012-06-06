#ifndef TAIR_FLOW_VIEW_H_
#define TAIR_FLOW_VIEW_H_

#include "base_packet.hpp"
#include "flowrate.h"

namespace tair {

class flow_view_request : public base_packet 
{
 public:
  flow_view_request()
  {
    setPCode(TAIR_REQ_FLOW_VIEW);
  }
 
  size_t size() 
  {
    return 4 + 16; // 16bytes tbnet packet header
  }

  bool encode(tbnet::DataBuffer *output)
  {
    output->writeInt16(ns);
    return true;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
  {
    ns = input->readInt32();
    return true;
  }

  uint32_t getNamespace()
  {
    return ns;
  }

 private:
  uint32_t ns;
};

class flow_view_response : public base_packet
{
 public:
  flow_view_response()
  {
    setPCode(TAIR_RESP_FLOW_VIEW);
  }
 
  size_t size() 
  {
    return 16 + 16; // 16bytes tbnet packet header
  }

  bool encode(tbnet::DataBuffer *output)
  {
    output->writeInt32(rate.in);
    output->writeInt32(rate.out);
    output->writeInt32(rate.cnt);
    output->writeInt32(rate.status);
    return true;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
  {
    if (header->_dataLen < 16)
      return false;
    rate.in = input->readInt32();
    rate.out = input->readInt32();
    rate.cnt = input->readInt32();
    rate.status = static_cast<tair::stat::FlowStatus>(input->readInt32());
    return true;
  }

  void set_flowrate(const tair::stat::Flowrate &rate)
  {
    this->rate = rate;
  }

 private:
  tair::stat::Flowrate rate;
};

}

#endif

