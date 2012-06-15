#include <cassert>
#include <string.h>
#include <errno.h>

#include "log.hpp"
#include "flow_controller_impl.h"
#include "mutex_lock.h"

#define ASSERTNS(ns) do { \
  assert(ns >= 0);        \
  assert(ns <= MAXAREA);   \
} while (false) 

namespace tair
{
namespace stat
{

inline
const char * FlowTypeStr(FlowType type) 
{
  switch (type) 
  {
   case IN:
     return "IN";
   case OUT:
     return "OUT";
   case OPS:
     return "OPS";
  }
  return "UNKONW";
}

inline
const char * FlowStatusStr(FlowStatus status)
{
  switch (status)
  {
   case UP:
     return "UP";
   case DOWN:
     return "DOWN";
   case KEEP:
     return "KEEP";
  }
  return "UNKONW";
}

FlowControllerImpl::FlowControllerImpl(int ns)
    : stop_(false),
      cal_interval_second_(2),
      mutex_(new flstorage::Mutex())
{
  bzero(flows_, sizeof(flows_));
  SetFlowLimit(-1, IN, DEFAULT_NET_LOWER, DEFAULT_NET_UPPER);
  SetFlowLimit(-1, OUT, DEFAULT_NET_LOWER, DEFAULT_NET_UPPER);
  SetFlowLimit(-1, OPS, DEFAULT_OPS_LOWER, DEFAULT_OPS_UPPER);
  SetFlowLimit(MAXAREA, IN, DEFAULT_TOTAL_NET_LOWER, DEFAULT_TOTAL_NET_UPPER);
  SetFlowLimit(MAXAREA, OUT, DEFAULT_TOTAL_NET_LOWER, DEFAULT_TOTAL_NET_UPPER);
  SetFlowLimit(MAXAREA, OPS, DEFAULT_TOTAL_OPS_LOWER, DEFAULT_TOTAL_OPS_UPPER);
}

FlowController* FlowController::NewInstance() 
{
  return new FlowControllerImpl(0);
}

bool FlowControllerImpl::Initialize() {
  int ret = pthread_create(&bg_tid, NULL, BackgroundFunc, this);
  if (ret != 0) 
  {
    log_error("create FlowController background thread faild: %s", strerror(errno));
  }
  return ret == 0;
}

FlowControllerImpl::~FlowControllerImpl()
{
  mutex_->Lock();
  stop_ = true;
  mutex_->Signal();
  mutex_->UnLock();

  pthread_join(bg_tid, NULL);
}

inline
FlowStatus FlowControllerImpl::IsOverflow(int ns)
{
  ASSERTNS(ns);
  return flows_[ns].status; 
}

Flowrate FlowControllerImpl::GetFlowrate(int ns)
{
  ASSERTNS(ns);
  AreaFlow &flow = flows_[ns];
  Flowrate rate = {atomic_read(&flow.in.last_per_second), 
                   flow.in.status,
                   atomic_read(&flow.out.last_per_second), 
                   flow.out.status,
                   atomic_read(&flow.ops.last_per_second),
                   flow.ops.status,
                   flows_[ns].status};
  return rate;
}

/**
 * @return ture is relaxed, (flow.status == DOWN)
 */
bool FlowControllerImpl::AddUp(Flow &flow, int size)
{
  if (atomic_read(&flow.curt_quantity) + size < 0)
  {
    // integer overflow
    flow.status = UP;
  }
  else
  {
    int curt = atomic_add_return(size, &flow.curt_quantity);
    if (curt < 0) {
      atomic_sub(size, &flow.curt_quantity);
      flow.status = UP; // integer overflow
    }
    if (curt / cal_interval_second_ >= atomic_read(&flow.upper_bound))
      flow.status = UP;
  }
  // curt / spend
  return flow.status == DOWN;
}

FlowStatus FlowControllerImpl::CalCurrentFlow(Flow &flow)
{
  // step
  // Atomic read time and quantity 
  int test_v1 = 0;
  int test_v2 = 0;
  timeval val;
  do {
    test_v1 = atomic_read(&flow.curt_quantity);
    gettimeofday(&val, NULL);
    test_v2 = atomic_read(&flow.curt_quantity);
  } while (test_v1 != test_v2 && false); // similar value enough
  int quantity = test_v1 + (test_v2 - test_v1) / 2;
  //Atomic END

  int now = (val.tv_sec % 100000) * 1000 + val.tv_usec / 1000; // 32bit, so mod 100000
  int last_cal_time = atomic_read(&flow.last_cal_time);
  int spend = last_cal_time > 0 ? 
                now - last_cal_time : 
                cal_interval_second_ * 1000;
  int last_per_second = static_cast<int>(spend < 1000.00001 ?
                          quantity : 
                          static_cast<double>(quantity) / spend * 1000);
  // stroe last flow for query 
  atomic_set(&flow.last_per_second, last_per_second);
  // last update time
  atomic_set(&flow.last_cal_time, now);
  // reset quantity
  atomic_sub(quantity, &flow.curt_quantity);

  if (last_per_second >= atomic_read(&flow.upper_bound))
    flow.status = UP;
  else if (flow.status == UP && last_per_second >= atomic_read(&flow.lower_bound))
    flow.status = KEEP;
  else if (last_per_second < atomic_read(&flow.lower_bound))
    flow.status = DOWN;
  return flow.status;
}

void* FlowControllerImpl::BackgroundFunc(void *arg)
{
  FlowControllerImpl *this_object = reinterpret_cast<FlowControllerImpl *>(arg);
  this_object->BackgroundCalFlows();
  return NULL;
}

void FlowControllerImpl::BackgroundCalFlows()
{
  while (stop_ == false)
  {
    mutex_->Lock();
    if (stop_ == false)
      mutex_->Wait(cal_interval_second_); // 
    mutex_->UnLock();
    if (stop_)
      break;
    log_debug("calculating flow");
    for (size_t ns = 0; ns < MAXAREA; ++ns)
    {
      AreaFlow &flow  = flows_[ns];
      FlowStatus status_in = CalCurrentFlow(flow.in);
      if (status_in != DOWN)
        log_info("Flow IN Limit: ns %d %s", ns, FlowStatusStr(status_in));
      FlowStatus status_out = CalCurrentFlow(flow.out);
      if (status_out != DOWN)
        log_info("Flow OUT Limit: ns %d %s", ns, FlowStatusStr(status_out));
      FlowStatus status_ops = CalCurrentFlow(flow.ops);
      if (status_ops != DOWN)
        log_info("Flow IN Limit: ns %d %s", ns, FlowStatusStr(status_ops));

      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG && 
          (atomic_read(&flow.in.last_per_second) != 0 ||
          atomic_read(&flow.out.last_per_second) != 0 ||
          atomic_read(&flow.ops.last_per_second) != 0))
      {
        log_debug("Flow rate: ns %d; in %d %s; out %d %s; ops %d %s",
            ns,
            atomic_read(&flow.in.last_per_second), FlowStatusStr(status_in),
            atomic_read(&flow.out.last_per_second), FlowStatusStr(status_out),
            atomic_read(&flow.ops.last_per_second), FlowStatusStr(status_ops)
         );
      }

      FlowStatus temp = status_in > status_out ? status_in : status_out;
      flow.status = temp > status_ops ? temp : status_ops;
    }
    log_debug("calculating end");
  }
}

bool FlowControllerImpl::AddUp(int ns, int in, int out)
{
  ASSERTNS(ns);
  // AddUp total first
  bool is_relaxed = false;
  if (ns != MAXAREA)
  {
    is_relaxed = AddUp(MAXAREA, in, out);
  }
  AreaFlow &flow = flows_[ns];
  bool limit = false;
  limit = !AddUp(flow.in, in) || limit; 
  limit = !AddUp(flow.out, out) || limit; 
  limit = !AddUp(flow.ops, 1) || limit; 
  if (limit == true)
    flow.status = UP;
  log_debug("Add up: ns %d, in %d, out %d, status %s", 
      ns, in, out, FlowStatusStr(flow.status));
  return is_relaxed || flow.status == DOWN;
}

FlowLimit FlowControllerImpl::GetFlowLimit(int ns, FlowType type)
{
  if (ns == -1) {
    FlowLimit bound = {0, 0};
    return bound;
  }

  ASSERTNS(ns);
  FlowLimit bound = {0, 0};
  AreaFlow &flow = flows_[ns];
  switch (type)
  {
   case IN:
    bound.lower = atomic_read(&flow.in.lower_bound);
    bound.upper = atomic_read(&flow.in.upper_bound);
    break;

   case OUT:
    bound.lower = atomic_read(&flow.out.lower_bound);
    bound.upper = atomic_read(&flow.out.upper_bound);
    break;

   case OPS:
    bound.lower = atomic_read(&flow.ops.lower_bound);
    bound.upper = atomic_read(&flow.ops.upper_bound);
    break;
  }
  return bound;
}

bool FlowControllerImpl::SetFlowLimit(int ns, FlowType type, int lower, int upper)
{
  if (lower < -1 || upper < -1) 
  {
    log_error("invliad flow limit parameter, lower %d; upper %d", lower, upper);
    return false;
  }
  if (ns == -1)
  {
    bool success = true;
    for (size_t i = 0; i < MAXAREA; ++i)
      if (SetFlowLimit(i, type, lower, upper) == false)
        success = false;
    return success;
  } 
  else
  {
    FlowLimit limit = GetFlowLimit(ns, type);
    if (upper != -1)
      limit.upper = upper;
    if (lower != -1)
      limit.lower = lower;

    if (limit.lower > limit.upper)
    {
      log_error("%d > %d is invalid", limit.lower, limit.upper);
      return false;
    }

    ASSERTNS(ns);
    log_info("set(or get) flow limit bound, ns %d; type %s; lower %d; upper %d", 
        ns, FlowTypeStr(type), lower, upper);
    switch (type) 
    {
     case IN:
      if (lower >= 0)
        atomic_set(&flows_[ns].in.lower_bound, lower);
      if (upper >= 0)
        atomic_set(&flows_[ns].in.upper_bound, upper);
      break;
     case OUT:
      if (lower >= 0)
        atomic_set(&flows_[ns].out.lower_bound, lower);
      if (upper >= 0)
        atomic_set(&flows_[ns].out.upper_bound, upper);
      break;
     case OPS:
      if (lower >= 0)
        atomic_set(&flows_[ns].ops.lower_bound, lower);
      if (upper >= 0)
        atomic_set(&flows_[ns].ops.upper_bound, upper);
      break;
     default:
      log_error("unknow type %d", type);
      break;
    }
  }
  return true;
}

}
}

