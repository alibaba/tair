#include "inval_async_task_thread.hpp"

namespace tair {
  AsyncTaskThread::AsyncTaskThread() {
    invalid_loader = NULL;
    retry_thread = NULL;
  }

  AsyncTaskThread::~AsyncTaskThread() {
  }

  void AsyncTaskThread::setThreadParameter(InvalLoader *loader,
      InvalRetryThread *retry_thread, RequestProcessor *processor, int thread_count) {
    this->invalid_loader = loader;
    this->retry_thread = retry_thread;
    this->processor = processor;
    setThreadCount(thread_count);
  }

  void AsyncTaskThread::stop() {
    CDefaultRunnable::stop();
    queue_cond.broadcast();
  }

  void AsyncTaskThread::run(tbsys::CThread *thread, void *arg) {
    int index = (int)((long)arg);
    log_warn("AsyncTaskThread %d starts.", index);

    while (!_stop) {
      base_packet *bp = NULL;

      queue_cond.lock();
      //~ wait until request is available, or stopped.
      while (!_stop && async_queue.size() == 0) {
        queue_cond.wait();
      }
      if (_stop) {
        queue_cond.unlock();
        break;
      }

      bp = (base_packet*)async_queue.pop();
			log_debug("async_queue.size: %d", async_queue.size());
      queue_cond.unlock();
      int pc = bp->getPCode();

      switch (pc) {
        case TAIR_REQ_INVAL_PACKET:
        {
          request_invalid *req = (request_invalid*) bp;
          request_invalid *post_req = NULL;
          processor->process(req, post_req);
          if (post_req != NULL) {
            log_error("async invalid failed, add packet to RetryThread %d", 0);
            retry_thread->add_packet(post_req, 0);
          }
          delete req;
          break;
        }
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
        {
          request_hide_by_proxy *req = (request_hide_by_proxy*) bp;
          request_hide_by_proxy *post_req = NULL;
          processor->process(req, post_req);
          if (post_req != NULL) {
            log_error("async hide failed, add packet to RetryThread %d", 0);
            retry_thread->add_packet(post_req, 0);
          }
          delete req;
          break;
        }
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
        {
          request_prefix_hides_by_proxy *req = (request_prefix_hides_by_proxy*) bp;
          request_prefix_hides_by_proxy *post_req = NULL;
          processor->process(req, post_req);
          if (post_req != NULL) {
            log_error("async prefix hides failed, add packet to RetryThread %d", 0);
            retry_thread->add_packet(post_req, 0);
          }
          delete req;
          break;
        }
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
        {
          request_prefix_invalids *req = (request_prefix_invalids*) bp;
          request_prefix_invalids *post_req = NULL;
          processor->process(req, post_req);
          if (post_req != NULL) {
            log_error("async prefix invalids failed, add packet to RetryThread %d", 0);
            retry_thread->add_packet(post_req, 0);
          }
          delete req;
          break;
        }
        default:
        {
          log_error("unknown packet with code %d", pc);
          delete bp;
          break;
        }
      }
    }
    //~ clear the queue when stopped.
    queue_cond.lock();
    while (async_queue.size() > 0) {
      delete async_queue.pop();
    }
    queue_cond.unlock();
    log_warn("AsyncTaskThread %d is stopped", index);
  }

  bool AsyncTaskThread::add_packet(base_packet *packet) {
    queue_cond.lock();
    if (async_queue.size() >= MAX_QUEUE_SIZE) {
      log_error(" AsyncTaskThread has overflowed, packet is dropped.");
      delete packet;
      queue_cond.unlock();
      return false;
    }
    log_debug("add packet to AsyncTaskThread");
    async_queue.push(packet);
    queue_cond.unlock();
    queue_cond.signal();
    return true;
  }
}
