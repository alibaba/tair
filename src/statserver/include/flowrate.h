#ifndef FLOW_RATE_H
#define FLOW_RATE_H

#include <cstddef>
#include <stdint.h>

namespace tair
{
namespace stat
{

enum FlowType
{
  IN, OUT, OPS
};

enum FlowStatus
{
  DOWN = 0, KEEP = 1, UP = 2 
};

struct FlowLimit 
{
  int lower;
  int upper;
};

struct Flowrate 
{
  uint32_t in;
  FlowStatus in_status;
  uint32_t out;
  FlowStatus out_status;
  uint32_t ops;
  FlowStatus ops_status;

  FlowStatus summary_status;
};

}
}

#endif

