#include "inval_async_task_thread.hpp"

namespace tair {
  AsyncTaskThread::AsyncTaskThread() {
    invalid_loader = NULL;
    retry_thread = NULL;
  }

  AsyncTaskThread::~AsyncTaskThread() {
  }

  void AsyncTaskThread::setThreadParameter(InvalLoader *loader, InvalRetryThread *retry_thread, int thread_count) {
    this->invalid_loader = loader;
    this->retry_thread = retry_thread;
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
      data_entry *key = NULL;
      base_packet *bp = NULL;
      request_hide_by_proxy *hpacket = NULL;
      request_invalid *ipacket = NULL;

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
      queue_cond.unlock();
      int pc = bp->getPCode();
      if (pc == TAIR_REQ_HIDE_BY_PROXY_PACKET) {
        hpacket = dynamic_cast<request_hide_by_proxy*>(bp);
      } else if (pc == TAIR_REQ_INVAL_PACKET) {
        ipacket = dynamic_cast<request_invalid*>(bp);
      }
      if (hpacket == NULL && ipacket == NULL) {
        log_error("unrecognized packet received, pcode: %d", pc);
        delete bp;
        continue;
      }

      std::vector<tair_client_api*> *clients =
        invalid_loader->get_client_list(PACKET_GROUP_NAME(ipacket, hpacket));
      int ret = TAIR_RETURN_SUCCESS;
      bool indicator = true;
      if (clients != NULL) {
        for (size_t i = 0; i < clients->size(); ++i) {
          tair_client_api *client = (*clients)[i];
          if (ipacket != NULL) {
            key = ipacket->key;
            ret = client->remove(ipacket->area, *key);
          }
          if (hpacket != NULL) {
            key = hpacket->key;
            ret = client->hide(hpacket->area, *key);
          }
          if (ret != TAIR_RETURN_SUCCESS) {
            if (ret != TAIR_RETURN_DATA_NOT_EXIST) {
              std::vector<std::string> servers;
              client->get_server_with_key(*key, servers);
              log_error("async %s failed: Group %s, DataServer %s.",
                  ipacket ? "invalid" : "hide", PACKET_GROUP_NAME(ipacket, hpacket), servers[0].c_str());
            } else {
              //~ if 'data_not_exist', regarded to be 'success'.
              ret = TAIR_RETURN_SUCCESS;
            }
          } else {
            log_debug("async %s successful", ipacket ? "invalid" : "hide");
          }
          if (ret != TAIR_RETURN_SUCCESS) {
            indicator = false;
          }
        }
        //~ if any requst failed, then retry.
        if (indicator == false) {
          ret = TAIR_RETURN_FAILED;
        }
      } else {
        log_error(" cannot find clients, maybe Group %s not added, or invalid_loader still loading.",
            PACKET_GROUP_NAME(ipacket, hpacket));
        ret = TAIR_RETURN_FAILED;
      }
      //~ retry
      if (ret != TAIR_RETURN_SUCCESS) {
        retry_thread->add_packet(bp, 0);
      } else {
        delete bp;
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
