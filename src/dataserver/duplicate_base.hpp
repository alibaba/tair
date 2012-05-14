/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_DUPLICATE_BASE_H
#define TAIR_DUPLICATE_BASE_H
#include <vector>
#include "base_packet.hpp"
#include "data_entry.hpp"

namespace tair{
   class base_duplicator {
   public:
      base_duplicator(){};
      virtual ~base_duplicator(){};
      virtual bool has_bucket_duplicate_done(int bucket_number) = 0;
      virtual void set_max_queue_size(uint32_t max_queue_size)=0;
      virtual void do_hash_table_changed()=0;
      virtual bool is_bucket_available(uint32_t bucket_id)=0;
   public:
      virtual int duplicate_data(int area, const data_entry* key, const data_entry* value,int expire_time,
                          int bucket_number, const std::vector<uint64_t>& des_server_ids,base_packet *request,int version)=0;
      virtual int direct_send(int area, const data_entry* key, const data_entry* value,int  expire_time,
            int bucket_number, const vector<uint64_t>& des_server_ids,uint32_t max_packet_id)=0;
   };
}
#endif
