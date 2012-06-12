#include "stat_processor.h"
#include "response_return_packet.hpp"

#include "stat_cmd_packet.hpp"


using namespace tair::stat;

namespace tair
{

stat_processor::stat_processor(tair_manager *tair_mgr, 
        StatManager *stat_mgr, FlowController *flow_ctrl)
    : tair_mgr(tair_mgr), 
      stat_mgr(stat_mgr),
      flow_ctrl(flow_ctrl)
{
}

uint32_t stat_processor::send_response(base_packet *request, 
                                       base_packet *response, 
                                       bool control_packet)
{
  assert(request != NULL);
  assert(response != NULL);
  if (control_packet)
    response->setChannelId(-1);
  else
    response->setChannelId(request->getChannelId());
  int size = response->size();
  if(request->get_connection()->postPacket(response) == false) 
  {
    delete response;
    size = 0;
  }
  return size;
}

uint32_t stat_processor::process(stat_cmd_view_packet *request)
{
  stat_arg arg = request->get_arg();
  tair::stat::Flowrate rate = stat_mgr->Measure(arg.ip, arg.ns, arg.op);
  arg.in  = rate.in;
  arg.out = rate.out;
  arg.cnt = rate.ops;
  stat_cmd_view_packet *response = new stat_cmd_view_packet();
  response->set_arg(arg);
  return send_response(request, response);
}

uint32_t stat_processor::process(flow_view_request *request)
{
  int view_ns = request->getNamespace();
  Flowrate rate = flow_ctrl->GetFlowrate(view_ns);   
  flow_view_response *response = new flow_view_response();
  response->setFlowrate(rate);
  return send_response(request, response);
}

uint32_t stat_processor::process(flow_check *request)
{
  int ns = request->getNamespace();
  FlowStatus status = flow_ctrl->IsOverflow(ns);
  flow_control *response = new flow_control();
  response->set_ns(ns);
  response->set_status(status);
  return send_response(request, response, true);
}

uint32_t stat_processor::process(flow_control_set *request)
{
  bool ret = flow_ctrl->SetFlowLimit(request->getNamespace(),
                          request->getType(),
                          request->getLower(),
                          request->getUpper());
  flow_control_set *response = new flow_control_set();
  response->setSuccess(ret);
  if (request->getNamespace() != -1) {
    FlowLimit limit = flow_ctrl->GetFlowLimit(request->getNamespace(), request->getType());
    response->setType(request->getType());
    response->setLimit(limit.lower, limit.upper);
    response->setNamespace(request->getNamespace());
  } else {
    *response = *request;
  }
  return send_response(request, response);
}

}

