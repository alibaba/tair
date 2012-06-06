#ifndef FLOW_CONTROLLER_H
#define FLOW_CONTROLLER_H

#include "flowrate.h"

namespace tair
{
namespace stat 
{

class FlowController
{
 public:
  static FlowController* NewInstance();

  virtual ~FlowController() {}

  virtual bool Initialize() = 0;

  virtual FlowStatus IsOverflow(int ns) = 0;

  virtual Flowrate GetFlowrate(int ns) = 0;

  virtual LimitBound GetLimitBound(int ns, FlowType type) = 0;

  /**
   * @return true is relaxed
   */
  virtual bool AddUp(int ns, int in, int out) = 0;

  virtual void SetFlowLimit(int ns, FlowType type, int lower, int upper) = 0;
};

}
}

#endif

