#ifndef TAIR_FLOW_CONTROL_PACKET_HPP
#define TAIR_FLOW_CONTROL_PACKET_HPP

#include "base_packet.hpp"
#include "flowrate.h"

namespace tair {

class flow_control_set: public base_packet
{
 public:
  flow_control_set() : type(tair::stat::IN), 
                       lower(-1), 
                       upper(-1), 
                       ns(-1), 
                       success(false)
  {
    setPCode(TAIR_FLOW_CONTROL_SET);
  }

  flow_control_set & operator=(const flow_control_set& rhs) 
  {
    this->type  = rhs.type;
    this->lower = rhs.lower;
    this->upper = rhs.upper;
    this->ns    = rhs.ns;
    this->success = rhs.success;
    return *this;
  }

  size_t size()
  {
    return 20 + 16;
  }

  bool encode(tbnet::DataBuffer *output)
  {
    output->writeInt32(type);
    output->writeInt32(lower);
    output->writeInt32(upper);
    output->writeInt32(ns);
    output->writeInt32(success);
    return true;
  }

  bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader* header)
  {
    if (header->_dataLen < 20)
      return false;

    type = static_cast<tair::stat::FlowType>(
        input->readInt32());
    lower = input->readInt32();
    upper = input->readInt32();
    ns = input->readInt32();
    success = input->readInt32();
    return true;
  }

  tair::stat::FlowType getType() { return type; }
  int32_t getLower() { return lower; }
  int32_t getUpper() { return upper; }
  int32_t getNamespace() { return ns; }

  void setType(tair::stat::FlowType type)
  {
    this->type = type;
  }

  void setLimit(int lower, int upper)
  {
    this->lower = lower;
    this->upper = upper;
  }

  void setNamespace(int ns)
  {
    this->ns = ns;
  }

  void setSuccess(bool f) 
  {
    this->success = f;
  }

  bool isSuccess() 
  { 
    return this->success;
  }

 private:
  tair::stat::FlowType type;
  int32_t lower;
  int32_t upper;
  int32_t ns;
  bool    success;
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
    if (header->_dataLen < 8)
      return false;
    status = input->readInt32();
    ns = input->readInt32();
    return true;
  }

  void set_status(int s)
  {
    this->status = s;
  }

  void set_ns(int ns)
  {
    this->ns = ns;
  }

  int get_ns()
  { 
    return ns;
  }

  int get_status()
  {
    return status;
  }

 private:
  int ns;
  int status;
};

}

#endif

