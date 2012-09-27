#include "inval_request_storage.hpp"
#include "retry_all_packet.hpp"
#include "invalid_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "hide_by_proxy_packet.hpp"
  namespace tair {
    inval_request_storage::inval_request_storage()
    {
      read_disk_threshold = 0.0;
      write_disk_threshold = 0.0;
      read_thresh = 0;
      write_thresh = 0;
      disk_queue = NULL;
      data_buffer = NULL;
      packet_header = NULL;
      packet_factory = NULL;
      max_cached_packet_count = 0;
    }

    inval_request_storage::~inval_request_storage()
    {
      //will do nothing, here.
      if (disk_queue != NULL) {
        delete disk_queue;
        disk_queue = NULL;
      }
      if (packet_header != NULL) {
        delete packet_header;
        packet_header = NULL;
      }
      if (data_buffer != NULL) {
        delete data_buffer;
      }
    }

    void inval_request_storage::setThreadParameter(const std::string& datapath, const std::string& queuename,
        const float read_disk_thresh, const float write_disk_thresh, const int this_max_cached_packet_count,
        tair_packet_factory *this_packet_factory)
    {
      read_disk_threshold = read_disk_thresh;
      read_thresh = this_max_cached_packet_count * read_disk_threshold;
      write_disk_threshold = write_disk_thresh;
      write_thresh = this_max_cached_packet_count * write_disk_threshold;
      data_path = datapath;
      queue_name = queuename;
      disk_queue = NULL;
      max_cached_packet_count = this_max_cached_packet_count;
      packet_factory = this_packet_factory;
    }

    int inval_request_storage::open_disk_queue()
    {
      int ret = RETURN_SUCCESS;
      packet_count_on_disk = 0;
      if (disk_queue == NULL) {
        char *pdata_path = const_cast<char*>(data_path.c_str());
        char *pqueue_name = const_cast<char*>(queue_name.c_str());
        log_debug("disk queue's data path: %s, name: %s", pdata_path, pqueue_name);
        disk_queue = new tbsys::CFileQueue(pdata_path, pqueue_name, 1024 * 1024 * 16); //16M
        data_buffer = new tbnet::DataBuffer();
        data_buffer->ensureFree(DATA_BUFFER_SIZE);
        packet_header = new tbnet::PacketHeader();
        packet_header->_dataLen = DATA_PACKET_MIN_SIZE;
      }
      else {
        log_error("disk_queue is exist!");
        ret = RETURN_FAILED;
      }
      return ret;
    }

    //stop the service
    void inval_request_storage::stop()
    {
      CDefaultRunnable::stop();
      queue_cond.signal();
    }

    //to encode the packet
    int inval_request_storage::encode_packet(tbnet::DataBuffer* &buffer, base_packet *packet)
    {
      int ret = RETURN_SUCCESS;
      buffer->clear();
      if (packet != NULL) {
        //write packet packet header
        buffer->writeInt32(packet->getPCode());
        //encode
        if (packet->encode(buffer) == false) {
          log_error("[FATAL ERROR] failed to encode packet, pcode: %d", packet->getPCode());
          ret = RETURN_FAILED;
        }
      }
      else {
        log_error("exception, packet is null");
        ret = RETURN_FAILED;
      }
      return ret;
    }

    //decode the packet
    int inval_request_storage::decode_packet(base_packet* &packet, tbsys::queue_item *item)
    {
      int ret = RETURN_SUCCESS;
      if (item != NULL) {
        //copy data, abtain the packet pcode
        data_buffer->clear();
        memcpy(data_buffer->getFree(), item->data, item->len);
        data_buffer->pourData(item->len);
        int pcode = data_buffer->readInt32();
        //create packet
        packet = (base_packet*)packet_factory->createPacket(pcode);
        if (packet != NULL) {
          if (packet->decode(data_buffer, packet_header) == false) {
            log_error("[FATAL ERROR] failed decode packet, packet pcode: %d", pcode);
            ret = RETURN_FAILED;
          }
        }
        else {
          log_error("failed create packet, pcode: %d", pcode);
          ret = RETURN_FAILED;
        }
      }
      else {
        log_error("error, item is null");
        ret = RETURN_FAILED;
      }
      return ret;
    }

    void inval_request_storage::run(tbsys::CThread* thread, void *arg)
    {
      log_info("inval_request_storage thread start...");
      if (open_disk_queue() == RETURN_FAILED) {
        log_error("[FATAL ERROR] failed open the disk_queue, stop the thread.");
        _stop = true;
      }
      while (!_stop) {
        queue_cond.lock();
        int size = request_queue.size();
        if (!_stop && ((read_thresh <= size && size <= write_thresh)
              || (size < read_thresh && disk_queue->isEmpty()))) {
          queue_cond.wait();
        }
        if (_stop) {
          queue_cond.unlock();
          break;
        }
        size = request_queue.size();
        if (size < read_thresh) {
          read_packet_from_disk();
        }
        else {
          write_packet_to_disk();
        }
        queue_cond.unlock();
      }
      write_packet_to_disk(true);
    }

    // read packet from disk, invoked by function run.
    int inval_request_storage::read_packet_from_disk()
    {
      if (request_queue.size() > read_thresh) {
        return request_queue.size();
      }
      // read from the file queue.
      int read_packet_count = max_cached_packet_count * ((read_disk_threshold + write_disk_threshold) / 2.0)
        - request_queue.size();
      base_packet *packet = NULL;
      int ret = RETURN_FAILED;
      for (int i = 0; ((i < read_packet_count) && (disk_queue->isEmpty() == false)); i++) {
        tbsys::queue_item *item = disk_queue->pop();
        if (item == NULL) {
          log_error("Failed to read the data from the file queue.,queue is empty");
          continue;
        }
        if (ret = decode_packet(packet, item) == RETURN_SUCCESS) {
          request_queue.push(packet);
          packet_count_on_disk--;
        }
        else if (ret == RETURN_FAILED) {
          log_error("[FATAL ERROR] failed to decode the packet.");
        }
        free(item);
        item = NULL;
      }
      if (packet_count_on_disk < 0) {
        packet_count_on_disk = 0;
      }
      return request_queue.size() + packet_count_on_disk;
    }

    // write packet to the disk, invoked by function run.
    int inval_request_storage::write_packet_to_disk(bool write_all)
    {
      int write_packet_count = 0;
      if (write_all == false) {
        if (request_queue.size() < write_thresh) {
          return request_queue.size();
        }
        else {
          write_packet_count = request_queue.size() -
            max_cached_packet_count * ((read_disk_threshold + write_disk_threshold) / 2.0);
        }
      }
      else {
        //write all packets to the disk.
        write_packet_count = request_queue.size();
      }
      base_packet *packet = NULL;
      for (int i = 0; i < write_packet_count && request_queue.size() > 0;) {
        packet = (base_packet*)request_queue.pop();//safety
        if (packet == NULL) {
          log_error("exception, packet poped from request_queue is null");
          continue;
        }
        data_buffer->clear();
        if (encode_packet(data_buffer, packet) == RETURN_SUCCESS) {
          if (disk_queue->push(data_buffer->getData(), data_buffer->getDataLen()) == EXIT_SUCCESS) {
            packet_count_on_disk++;
          }
          else {
            log_error("[FATAL ERROR] failed to push data to disk queue, packet's pcode: %d", packet->getPCode());
          }
          //need virtual destructor.
          if (packet != NULL ) {
            delete packet;
          }
        }
        else {
          log_error("[FATAL ERROR], failed to encode the packet to the buffer. packet pcode: %d",packet->getPCode());
        }
        i++;
      }
      return request_queue.size() + packet_count_on_disk;
    }

    // the interface of request_storage, invoked by async_task_thread.
    int inval_request_storage::write_request(base_packet* bp)
    {
      int rest_packet_count = 0;
      queue_cond.lock();
      if (bp != NULL) {
        request_queue.push(bp);
      }
      else {
        log_error("exceptin, packet is null");
      }
      rest_packet_count = request_queue.size();
      queue_cond.unlock();

      if (rest_packet_count > write_thresh) {
        queue_cond.signal();
      }
      return rest_packet_count + packet_count_on_disk;
    }

    // the interface of request_storage, invoked by async_task_thread.
    int inval_request_storage::read_request(std::vector<base_packet*>& packet_vector, const int read_count)
    {
      int rest_packet_count = 0;
      packet_vector.clear();
      queue_cond.lock();
      for (int i = 0; (request_queue.size() > 0 && i < read_count);) {
        base_packet* packet = (base_packet*) request_queue.pop();
        if (packet != NULL) {
          packet_vector.push_back(packet);
        }
        else {
          log_error("exception, packet is null poped from the request_queue.");
          continue;
        }
        i++;
      }
      rest_packet_count = request_queue.size();
      queue_cond.unlock();

      if (rest_packet_count < read_thresh) {
        queue_cond.signal();
      }

      return rest_packet_count + packet_count_on_disk;
    }

    // get the sum of the packet count in the memory queue and on the disk
    int inval_request_storage::get_packet_count()
    {
      queue_cond.lock();
      int size = request_queue.size() + packet_count_on_disk;
      queue_cond.unlock();
      return size;
    }

    // get the count of packet on the disk.
    int inval_request_storage::get_packet_count_on_disk()
    {
      queue_cond.lock();
      int size = packet_count_on_disk;
      queue_cond.unlock();
      return size;
    }
  }
