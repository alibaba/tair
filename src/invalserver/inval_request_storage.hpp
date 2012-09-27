#ifndef INVAL_REQUEST_STORAGE_H
#define INVAL_REQUEST_STORAGE_H

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "log.hpp"
#include "base_packet.hpp"
#include <fstream>
#include <string>
#include "packet_factory.hpp"
namespace tair {
  class inval_request_storage : public tbsys::CDefaultRunnable {
  public:
    inval_request_storage();

    ~inval_request_storage();
  public:
    void setThreadParameter(const std::string& data_path, const std::string& queue_name,
        const float read_disk_threshold, const float write_disk_threshold, const int max_cached_packet_count,
        tair_packet_factory* packet_factory);

    void stop();

    void run(tbsys::CThread *thread, void *arg);

    int get_packet_count();

    int get_packet_count_on_disk();

    inline int get_queue_max_size()
    {
      return max_cached_packet_count;
    }

    inline int get_write_threshold()
    {
      return write_thresh;
    }

    inline int get_read_threshold()
    {
      return read_thresh;
    }

    int write_request(base_packet* packet);

    int read_request(std::vector<base_packet*>& packet_vector, const int read_count);
  private:
    int read_packet_from_disk();

    int write_packet_to_disk(bool write_all = false);

    int decode_packet(base_packet* &packet, tbsys::queue_item *item);

    int encode_packet(tbnet::DataBuffer* &buffer, base_packet* packet);

    int open_disk_queue();
  private:
    tbnet::PacketQueue request_queue;
    tbnet::PacketHeader *packet_header;
    tbnet::DataBuffer* data_buffer;
    tair_packet_factory *packet_factory;

    int max_cached_packet_count;
    static const uint32_t DATA_BUFFER_SIZE = 8192;//8k
    static const uint32_t DATA_PACKET_MIN_SIZE = 7;

    //if queue's size > write_disk_threshold * max_cached_packet_count, write data to the file.
    float write_disk_threshold;
    int write_thresh;
    //if queue's size < read_disk_threshold * max_cached_packet_count, read data from the file.
    float read_disk_threshold;
    int read_thresh;
    tbsys::CThreadCond queue_cond;
    tbsys::CFileQueue* disk_queue;
    enum { RETURN_SUCCESS = 0, RETURN_FAILED = 1 };
    std::string queue_name;
    std::string data_path;
    //for supporting debug
    int32_t packet_count_on_disk;
  };
}
#endif
