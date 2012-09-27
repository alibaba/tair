#include "inval_async_task_thread.hpp"
namespace tair {
  AsyncTaskThread::AsyncTaskThread() {
    invalid_loader = NULL;
    retry_thread = NULL;
  }

  AsyncTaskThread::~AsyncTaskThread() {
  }

  void AsyncTaskThread::setThreadParameter(InvalLoader *loader,
      InvalRetryThread *retry_thread,
      RequestProcessor *processor,
      int thread_count,
      inval_request_storage *req_storage) {
    this->invalid_loader = loader;
    this->retry_thread = retry_thread;
    this->processor = processor;
    this->request_storage = req_storage;
    setThreadCount(thread_count);
  }

  void AsyncTaskThread::stop() {
    CDefaultRunnable::stop();
    queue_cond.broadcast();
  }

  int AsyncTaskThread::async_queue_size()
  {
    queue_cond.lock();
    int size = async_queue.size();
    queue_cond.unlock();
    return size;
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
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::INVALID, std::string(req->group_name),
                req->area, inval_area_stat::FIRST_EXEC);
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
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::HIDE, std::string(req->group_name),
                req->area, inval_area_stat::FIRST_EXEC);
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
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::PREFIX_HIDE, std::string(req->group_name),
                req->area, inval_area_stat::FIRST_EXEC);
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
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::PREFIX_INVALID, std::string(req->group_name),
                req->area, inval_area_stat::FIRST_EXEC);
            if (post_req != NULL) {
              log_error("async prefix invalids failed, add packet to RetryThread %d", 0);
              retry_thread->add_packet(post_req, 0);
            }
            delete req;
            break;
          }
        case TAIR_REQ_RETRY_ALL_PACKET:
          {
            request_retry_all *req = (request_retry_all*) bp;
            do_retry_all_request(req);
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
  int AsyncTaskThread::do_retry_all_request(request_retry_all* req) {
    int limit_size = (int) (MAX_QUEUE_SIZE * safety_ratio); // safe
    int async_queue_size = async_queue.size();
    int retry_packet_count = limit_size - async_queue_size;
    int  pushed_packet_count = 0;
    if (retry_packet_count <= 0) {
      log_debug("async_queue size: %d", async_queue_size);
    }
    else {
      std::vector<base_packet*> packet_vector;
      int left_packet_count = 0;
      left_packet_count = request_storage->read_request(packet_vector, retry_packet_count);
      log_debug("left  %d packet(s), vector size :%d", left_packet_count, packet_vector.size());
      //push packet to the async_task_queue.
      for (int i = 0; i < packet_vector.size(); ++i) {
        base_packet *req = packet_vector[i];
        if (req != NULL) {
          //if failed to push the packet to the task queue,
          //the packet `req was pushed into local storage `request_storage by add_packet(function).
          if(!add_packet(req)) {
            log_error("failed to push the packet, packet pcode: %d", req->getPCode());
          }
          pushed_packet_count++;
        }
        else {
          log_error("[FATAL ERROR] packet is null ,read from the request storage.");
        }
      }
    }
    log_debug("push %d packet(s) to asyn task queue. current count of packets: %d",
        pushed_packet_count,request_storage->get_packet_count());
    if (request_storage->get_packet_count() > 0) {
      request_retry_all *retry_all = new request_retry_all();
      if (retry_all != NULL) {
        if(!add_packet(retry_all)) {
          log_error("failed to add retry all to  the async queue.");
          delete retry_all;
        }
      }
    }
    return request_storage->get_packet_count();
  }

  bool AsyncTaskThread::add_packet(base_packet *packet) {
    queue_cond.lock();
    if (async_queue.size() >= MAX_QUEUE_SIZE) {
      queue_cond.unlock();
      log_error(" AsyncTaskThread has overflowed, packet is cached into `request_storage.");
      request_storage->write_request(packet);
      return false;
    }
    async_queue.push(packet);
    queue_cond.unlock();
    queue_cond.signal();
    return true;
  }
}
