#ifndef TAIR_FLOW_CONTROL_PACKET_HPP
#define TAIR_FLOW_CONTROL_PACKET_HPP

#include "base_packet.hpp"
#include "flow_controller.h"

namespace tair {

class flow_control_set: public base_packet
{
 public:
  flow_control_set()
  {
    setPCode(TAIR_FLOW_CONTROL_SET);
  }

  size_t size()
  {
    return 16 + 16;
  }

  bool encode(tbnet::DataBuffer *output)
  {
    return false;
  }

  bool decode(tbnet::DataBuffer *input)
  {
    type = static_cast<tair::stat::FlowType>(
        input->readInt32());
    lower = input->readInt32();
    upper = input->readInt32();
    ns = input->readInt32();
    return true;
  }

  tair::stat::FlowType getType() { return type; }
  int32_t getLower() { return lower; }
  int32_t getUpper() { return upper; }
  int32_t getNamespace() { return ns; }

 private:
  tair::stat::FlowType type;
  int32_t lower;
  int32_t upper;
  int32_t ns;
};

class flow_check : public base_packet
{
 public:
  flow_check()
  {
    setPCode(TAIR_FLOW_CHECK);
  }

  size_t size()
  {
    return 4 + 16;
  }

  bool encode(tbnet::DataBuffer *output)
  {
    return false;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
  {
    ns = input->readInt32();
    return true;
  }

  int32_t getNamespace() 
  {
    return ns;
  }
 private:
  int32_t ns;
};

class flow_control : public base_packet 
{
 public:
  flow_control()
  {
    setPCode(TAIR_FLOW_CONTROL);
  }
 
  size_t size() 
  {
    return 8 + 16; // 16bytes tbnet packet header
  }

  bool encode(tbnet::DataBuffer *output)
  {
    output->writeInt32(status);
    output->writeInt32(ns);
    return true;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
  {
    return false;
  }

  void set_status(int s)
  {
    this->status = s;
  }

  void set_ns(int ns)
  {
    this->ns = ns;
  }

 private:
  int ns;
  int status;
};

}

#endif

