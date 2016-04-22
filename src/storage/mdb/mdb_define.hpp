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

#ifndef MDB_DEFINE_H_
#define MDB_DEFINE_H_

#include <stdint.h>

#define TAIR_SLAB_LARGEST            100
#define TAIR_SLAB_BLOCK              1048576
#define TAIR_SLAB_ALIGN_BYTES        8
#define TAIR_SLAB_HASH_MAXDEPTH      8

#include <map>
#include <sys/unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

struct mdb_param {
    static bool lock_pshared;
    static bool use_check_thread;
    static bool enable_put_remove_expired;
    static uint32_t inst_shift;
    static const char *mdb_type;
    static const char *mdb_path;
    static int64_t size;
    static int slab_base_size;
    static int page_size;
    static double factor;
    static int hash_shift;

    static int chkexprd_time_low;
    static int chkexprd_time_high;

    static int mem_merge_time_low;
    static int mem_merge_time_high;
    static int mem_merge_move_count;

    static int chkslab_time_low;
    static int chkslab_time_high;
    static int check_granularity;
    static int check_granularity_factor;

    static std::map<uint32_t, uint64_t> default_area_capacity;
};

bool hour_range(int min, int max);

char *open_shared_mem(const char *path, int64_t size);

double get_timespec();

#endif /*MDB_DEFINE_H_ */
