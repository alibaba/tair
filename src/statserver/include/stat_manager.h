#ifndef STAT_MANAGER_H
#define STAT_MANAGER_H

#include <cstddef> 
#include <cstdlib>
#include <stdint.h>
#include <string.h>
#include <cassert>

#include "flowrate.h"

namespace tair
{

namespace stat
{

struct StatRecord
{
  uint32_t ip;
  uint16_t ns;
  uint16_t op;
  uint32_t in;
  uint32_t out;

  StatRecord() : ip(0), ns(0), op(0), in(0), out(0) {}
};

class StatManager
{
 public:
  static StatManager* NewInstance();

  virtual ~StatManager() {}

  virtual bool Initialize() = 0;

  // return , true: out of flow, need flow control
  virtual void AddUp(uint32_t ip, uint16_t ns, uint16_t op, 
             uint32_t in, uint32_t out, uint32_t cnt = 1) = 0;

  virtual void AddUp(StatRecord rec, uint32_t cnt = 1)
  {
    AddUp(rec.ip, rec.ns, rec.op, rec.in, rec.out, cnt);
  }

  virtual Flowrate Measure(uint32_t ip, uint16_t ns, uint16_t op) = 0;

  virtual bool SwitchAndScheduleDump() = 0;
};

}
}

#endif

