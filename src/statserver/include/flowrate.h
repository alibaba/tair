#ifndef FLOW_RATE_H
#define FLOW_RATE_H

#include <cstddef>
#include <stdint.h>

namespace tair
{
namespace stat
{

struct Flowrate 
{
  uint32_t in;
  uint32_t out;
  uint32_t cnt;
};

}
}

#endif

