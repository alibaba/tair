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
  IN, OUT, CNT
};

enum FlowStatus
{
  DOWN = 0, KEEP = 1, UP = 2 
};

struct LimitBound
{
  int lower;
  int upper;
};

struct Flowrate 
{
  uint32_t in;
  uint32_t out;
  uint32_t cnt;
  FlowStatus status;
};

}
}

#endif

