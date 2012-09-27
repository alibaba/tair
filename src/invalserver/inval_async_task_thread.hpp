#ifndef INVAL_ASYNC_TASK_THREAD_HPP
#define INVAL_ASYNC_TASK_THREAD_HPP

#include <tbsys.h>
#include <tbnet.h>

#include "inval_loader.hpp"
#include "log.hpp"
#include "inval_retry_thread.hpp"
#include "base_packet.hpp"
#include "invalid_packet.hpp"
#include "hide_packet.hpp"
#include "inval_processor.hpp"
#include "prefix_invalids_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "inval_stat_helper.hpp"
#include "retry_all_packet.hpp"

namespace tair {
  class AsyncTaskThread: public tbsys::CDefaultRunnable {
  public:
    AsyncTaskThread();
    ~AsyncTaskThread();

    void setThreadParameter(InvalLoader *loader,
        InvalRetryThread *retry_thread,
        RequestProcessor *processor,
        int thread_count,
        inval_request_storage *request_storage);

    void stop();

    void run(tbsys::CThread *thread, void *arg);

    bool add_packet(base_packet *bp);

    int async_queue_size();
  private:
    int do_retry_all_request(request_retry_all* req);
  private:
    static const int MAX_QUEUE_SIZE = 10000;
    static const float safety_ratio = 0.8;
    InvalLoader *invalid_loader;
    InvalRetryThread *retry_thread;
    RequestProcessor *processor;
    inval_request_storage *request_storage; //from the inval_server
    tbsys::CThreadCond queue_cond;
    tbnet::PacketQueue async_queue;
  };
}
#endif
