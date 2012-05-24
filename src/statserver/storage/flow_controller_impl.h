#ifndef FLOW_CONTROLLER_IMPL_H
#define FLOW_CONTROLLER_IMPL_H

#include <time.h>
#include <pthread.h>

#include "flow_controller.h"
#include "atomic.h"

#define MAXAREA 1024

namespace flstorage
{
class Mutex;
}

namespace tair
{
namespace stat
{

struct Flow
{
  atomic_t curt_quantity;
  atomic_t last_per_second;
  atomic_t last_cal_time;
  atomic_t lower_bound;
  atomic_t upper_bound;
  volatile FlowStatus status;
};

struct AreaFlow
{
  Flow in;
  Flow out;
  Flow cnt;
  volatile FlowStatus status;
};

class FlowControllerImpl : public FlowController
{
 public:
  FlowControllerImpl(int n);
  virtual ~FlowControllerImpl();

  virtual Flowrate GetFlowrate(int ns);

  virtual FlowStatus IsOverflow(int ns);

  virtual bool AddUp(int ns, int in, int out);

  virtual void SetFlowLimit(int ns, FlowType type, int lower, int upper);

  virtual LimitBound GetLimitBound(int ns, FlowType type);

  virtual bool Initialize();

 private:
  void BackgroundCalFlows();
  FlowStatus CalCurrentFlow(Flow &flow);
  bool AddUp(Flow &flow, int size);

  static void* BackgroundFunc(void *arg);

  AreaFlow flows_[MAXAREA];  
  bool stop_;
  pthread_t bg_tid;

  time_t cal_interval_second_;
  flstorage::Mutex *mutex_;

 private:
  static const int DEFAULT_NET_UPPER = 30 * 1024 * 1024; // 30MB/s
  static const int DEFAULT_NET_LOWER = 15 * 1024 * 1024; // 15MB/s
  static const int DEFAULT_CNT_UPPER = 30000; // 30000/s
  static const int DEFAULT_CNT_LOWER = 30000; // 30000/s
};


}
}

#endif
