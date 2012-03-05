#include "inval_retry_thread.hpp"

namespace tair {
  InvalRetryThread::InvalRetryThread() {
    invalid_loader = NULL;
    setThreadCount(RETRY_COUNT);
  }

  InvalRetryThread::~InvalRetryThread() {
  }

  void InvalRetryThread::setThreadParameter(InvalLoader *loader) {
    invalid_loader = loader;
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
      data_entry *key = NULL;
      base_packet *bp = NULL;
      request_invalid *ipacket = NULL;
      request_hide_by_proxy *hpacket = NULL;
      cur_cond->lock();
      //~ wait until request is available, or stopped.
      while (!_stop && cur_queue->size() == 0) {
        cur_cond->wait();
      }
      if (_stop) {
        cur_cond->unlock();
        break;
      }

      bp = (request_invalid*)cur_queue->pop();
      cur_cond->unlock();
      int pc = bp->getPCode();
      if (pc == TAIR_REQ_INVAL_PACKET) {
        ipacket = dynamic_cast<request_invalid*>(bp);
      }
      if (pc == TAIR_REQ_HIDE_BY_PROXY_PACKET) {
        hpacket = dynamic_cast<request_hide_by_proxy*>(bp);
      }
      if (ipacket == NULL && hpacket == NULL) {
        delete bp;
        continue;
      }

      //~ wait
      int towait = bp->request_time + delay_time - time(NULL);
      if (towait > 0) {
        log_debug("wait for %d seconds to retry in RetryThread %d.", towait, index);
        TAIR_SLEEP(_stop, towait);
        if (_stop)
          break;
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
              log_error("Retry%sFailure: Thread %d, Group %s, DataServer %s.",
                  ipacket ? "Invalid" : "Hide", index, PACKET_GROUP_NAME(ipacket, hpacket), servers[0].c_str());
            } else {
              //~ if 'data_not_exist', regarded to be 'success'.
              ret = TAIR_RETURN_SUCCESS;
            }
          } else {
            log_warn("Retry%sSuccess in thread %d", ipacket ? "Invalid" : "Hide", index);
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
        log_error("[FATAL ERROR] cannot find clients, maybe Group %s not added, or invalid_loader still loading.",
            PACKET_GROUP_NAME(ipacket, hpacket));
        ret = TAIR_RETURN_FAILED;
      }
      //~ retry again, or give up
      if (ret != TAIR_RETURN_SUCCESS && index < RETRY_COUNT - 1) {
        add_packet(bp, index + 1);
      } else if (ret != TAIR_RETURN_SUCCESS) {
        log_error("[FATAL ERROR] RetryRemoveFailureFinally");
        delete bp;
      } else { //~ success
        delete bp;
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
