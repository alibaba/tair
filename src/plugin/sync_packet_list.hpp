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

#ifndef SYNC_PACKET_LIST_H
#define SYNC_PACKET_LIST_H

#include "BlockQueueEx.hpp"
#include "file_list_factory.hpp"
#include "sync_packet.hpp"
#include "scoped_wrlock.hpp"
#include "data_entry.hpp"

#ifdef TAIR_DEBUG
#define MAX_SYNC_LIST_SIZE  1024*100000
#else
#define MAX_SYNC_LIST_SIZE 2048*100000
#endif

#define LOG_FILE_NAME "/sync.list"

namespace tair {
using namespace tair::common;

enum {
    STATE_MEM_STORE = 0,
    STATE_DISK_STORE = 1,
    STATE_SWITCHING_STORE = 2
};

class sync_packet_list {
public:
    sync_packet_list();

    ~sync_packet_list();

public:
    bool enable_disk_save(const std::string &disk_save_name, bool replay = true);

public:
    bool isEmpty();

    int put(data_entry *&value);

    int put_head(data_entry *&value);

    data_entry *get(int timeout = 2000);//ms
    unsigned long size();

private:
    bool check_init();

    int do_batch_load(unsigned int _want_size);

private:
    //typedef __gnu_cxx::hash_map<uint32_t,T > CDuplicatPkgMap;
    //CDuplicatPkgMap m_PkgWaitMap;
    BlockQueueEx<data_entry *> m_in_queue;
    BlockQueueEx<data_entry *> *m_pout_queue;
    common::CSlotLocks *m_slots_locks;
    common::RecordLogFile <data_entry> m_disklist_file;
    atomic_t m_disk_stored;
    bool m_use_disk;
    std::string m_disk_path;
};

}
#endif
