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
    output->writeInt32(ns);
    return true;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
  {
    if (header->_dataLen < 4)
      return false;
    ns = input->readInt32();
    return true;
  }

  void setNamespace(int ns)
  {
    this->ns = ns;
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
    return 28 + 16; // 16bytes tbnet packet header
  }

  bool encode(tbnet::DataBuffer *output)
  {
    output->writeInt32(rate.in);
    output->writeInt32(rate.in_status);
    output->writeInt32(rate.out);
    output->writeInt32(rate.out_status);
    output->writeInt32(rate.ops);
    output->writeInt32(rate.ops_status);
    output->writeInt32(rate.status);
    return true;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
  {
    if (header->_dataLen < 28)
      return false;
    rate.in = input->readInt32();
    rate.in_status = static_cast<tair::stat::FlowStatus>(input->readInt32());
    rate.out = input->readInt32();
    rate.out_status = static_cast<tair::stat::FlowStatus>(input->readInt32());
    rate.ops = input->readInt32();
    rate.ops_status = static_cast<tair::stat::FlowStatus>(input->readInt32());
    rate.status = static_cast<tair::stat::FlowStatus>(input->readInt32());
    return true;
  }

  void setFlowrate(const tair::stat::Flowrate &rate)
  {
    this->rate = rate;
  }

  tair::stat::Flowrate getFlowrate() 
  {
    return this->rate;
  }

 private:
  tair::stat::Flowrate rate;
};

}

#endif

