#include "inval_retry_thread.hpp"

namespace tair {
  InvalRetryThread::InvalRetryThread() {
    invalid_loader = NULL;
    processor = NULL;
    setThreadCount(RETRY_COUNT);
  }

  InvalRetryThread::~InvalRetryThread() {
  }

  void InvalRetryThread::setThreadParameter(InvalLoader *loader, RequestProcessor *processor) {
    this->invalid_loader = loader;
    this->processor = processor;
  }

  void InvalRetryThread::stop() {
    CDefaultRunnable::stop();
    for (int i = 0; i < RETRY_COUNT; ++i) {
      queue_cond[i].broadcast();
    }
  }

  void InvalRetryThread::run(tbsys::CThread *thread, void *arg) {
    int index = (int)((long)arg);
    if (index < 0 || index > RETRY_COUNT - 1 || invalid_loader == NULL ) {
      return ;
    }
    log_warn("RetryThread %d starts.", index);

    tbsys::CThreadCond *cur_cond = &(queue_cond[index]);
    tbnet::PacketQueue *cur_queue = &(retry_queue[index]);

    int delay_time = index * 3 + 1;

    while (!_stop) {
      base_packet *bp = NULL;
      cur_cond->lock();
      //~ wait until request is available, or stopped.
      while (!_stop && cur_queue->size() == 0) {
        cur_cond->wait();
      }
      if (_stop) {
        cur_cond->unlock();
        break;
      }

      bp = (base_packet*) cur_queue->pop();
      cur_cond->unlock();
      int pc = bp->getPCode();

      int towait = bp->request_time + delay_time - time(NULL);
      if (towait > 0) {
        log_debug("wait for %d seconds to retry in RetryThread %d.", towait, index);
        TAIR_SLEEP(_stop, towait);
        if (_stop)
          break;
      }

      switch (pc) {
        case TAIR_REQ_INVAL_PACKET:
        {
          request_invalid *req = (request_invalid*) bp;
          request_invalid *post_req = NULL;
          processor->process(req, post_req);
          if (post_req != NULL) {
            if (index < RETRY_COUNT - 1) {
              log_error("add invalid packet to RetryThread %d", index + 1);
              add_packet(post_req, index + 1);
            } else {
              log_error("invalid RetryFailedFinally, keys would be dumped");
              char msg[256];
              snprintf(msg, sizeof(msg), "prefix invalid failed, group: %s, area: %d",
                  post_req->group_name, post_req->area);
              if (post_req->key != NULL) {
                processor->dump_key(*post_req->key, msg);
              } else if (post_req->key_list != NULL) {
                processor->dump_key(*post_req->key_list, msg);
              }
              delete post_req;
            }
          } else {
            log_error("invalid success in RetryThread %d", index);
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
            if (index < RETRY_COUNT - 1) {
              log_error("add hide packet to RetryThread %d", index + 1);
              add_packet(post_req, index + 1);
            } else {
              log_error("hide RetryFailedFinally, keys would be dumped");
              char msg[256];
              snprintf(msg, sizeof(msg), "prefix hide failed, group: %s, area: %d",
                  post_req->group_name, post_req->area);
              if (post_req->key != NULL) {
                processor->dump_key(*post_req->key, msg);
              } else if (post_req->key_list != NULL) {
                processor->dump_key(*post_req->key_list, msg);
              }
              delete post_req;
            }
          } else {
            log_error("hide success in RetryThread %d", index);
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
            if (index < RETRY_COUNT - 1) {
              log_error("add prefix hides packet to RetryThread %d", index + 1);
              add_packet(post_req, index + 1);
            } else {
              log_error("prefix hides RetryFailedFinally, keys would be dumped");
              char msg[256];
              snprintf(msg, sizeof(msg), "prefix hides failed, group: %s, area: %d",
                  post_req->group_name, post_req->area);
              if (post_req->key != NULL) {
                processor->dump_key(*post_req->key, msg);
              } else if (post_req->key_list != NULL) {
                processor->dump_key(*post_req->key_list, msg);
              }
              delete post_req;
            }
          } else {
            log_error("prefix hides success in RetryThread %d", index);
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
            if (index < RETRY_COUNT - 1) {
              log_error("add prefix invalids packet to RetryThread %d", index + 1);
              add_packet(post_req, index + 1);
            } else {
              log_error("prefix invalids RetryFailedFinally, keys would be dumped");
              char msg[256];
              snprintf(msg, sizeof(msg), "prefix invalids failed, group: %s, area: %d",
                  post_req->group_name, post_req->area);
              if (post_req->key != NULL) {
                processor->dump_key(*post_req->key, msg);
              } else if (post_req->key_list != NULL) {
                processor->dump_key(*post_req->key_list, msg);
              }
              delete post_req;
            }
          } else {
            log_error("prefix invalids success in RetryThread %d", index);
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
    cur_cond->lock();
    while (cur_queue->size() > 0) {
      delete cur_queue->pop();
    }
    cur_cond->unlock();
    log_warn("RetryThread %d is stopped", index);
  }

  void InvalRetryThread::add_packet(base_packet *packet, int index) {
    if (index < 0 || index > RETRY_COUNT - 1 || _stop == true) {
      log_error("add_packet failed: index: %d, _stop: %d", index, _stop);
      delete packet;
    }
    queue_cond[index].lock();
    if (retry_queue[index].size() >= MAX_QUEUE_SIZE) {
      queue_cond[index].unlock();
      log_error("[FATAL ERROR] Retry Queue %d has overflowed, packet is dropped.", index);
      delete packet;
      return ;
    }
    log_info("add packet to RetryThread %d", index);
    packet->request_time = time(NULL);
    retry_queue[index].push(packet);
    queue_cond[index].unlock();
    queue_cond[index].signal();
  }
}
