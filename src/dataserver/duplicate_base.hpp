/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef TAIR_DUPLICATE_BASE_H
#define TAIR_DUPLICATE_BASE_H

#include <vector>
#include "base_packet.hpp"
#include "data_entry.hpp"

namespace tair {
namespace common {
class Record;
}

class base_duplicator {
public:
    base_duplicator() {};

    virtual ~base_duplicator() {};

    virtual bool has_bucket_duplicate_done(int bucket_number) = 0;

    virtual void set_max_queue_size(uint32_t max_queue_size)=0;

    virtual void set_dup_timeout(int ms)=0;

    virtual void do_hash_table_changed()=0;

    virtual bool is_bucket_available(uint32_t bucket_id)=0;

public:
    virtual int duplicate_data(int area, const data_entry *key,
                               const data_entry *value, int bucket_number,
                               const std::vector<uint64_t> &des_server_ids, easy_request_t *r) = 0;

    virtual int duplicate_data(int32_t bucket, const std::vector<common::Record *> *records,
                               const std::vector<uint64_t> &des_server_ids, easy_request_t *r) {
        return TAIR_RETURN_DUPLICATE_SEND_ERROR;
    }

    // why put this function into interface ?
    virtual int direct_send(easy_request_t *r, int area, const data_entry *key,
                            const data_entry *value, int bucket_number,
                            const vector <uint64_t> &des_server_ids, uint32_t max_packet_id) = 0;

    virtual int duplicate_data(int32_t bucket_number, easy_request_t *r, vector <uint64_t> &slaves) {
        return TAIR_RETURN_DUPLICATE_SEND_ERROR;
    }

    virtual int duplicate_batch_data(int bucket_number, const mput_record_vec *record_vec,
                                     const std::vector<uint64_t> &des_server_ids,
                                     base_packet *request) { return TAIR_RETURN_SUCCESS; }
};
}
#endif
