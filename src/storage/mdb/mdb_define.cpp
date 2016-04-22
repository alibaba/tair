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

#include "mdb_define.hpp"
#include <algorithm>
#include <time.h>

bool mdb_param::use_check_thread = true;
bool mdb_param::lock_pshared = false;
bool mdb_param::enable_put_remove_expired = false;
uint32_t mdb_param::inst_shift = 3;
const char *mdb_param::mdb_type = "mdb_shm";
const char *mdb_param::mdb_path = "mdb_shm_path";
int64_t mdb_param::size = (1 << 31);
int mdb_param::page_size = (1 << 20);
double mdb_param::factor = 1.1;
int mdb_param::hash_shift = 21;
int mdb_param::slab_base_size = 64;

int mdb_param::chkexprd_time_low = 2;
int mdb_param::chkexprd_time_high = 4;

int mdb_param::chkslab_time_low = 5;
int mdb_param::chkslab_time_high = 7;

int mdb_param::mem_merge_time_low = 5;
int mdb_param::mem_merge_time_high = 6;
int mdb_param::mem_merge_move_count = 300;

int mdb_param::check_granularity = 15;
int mdb_param::check_granularity_factor = 10;

std::map<uint32_t, uint64_t>  mdb_param::default_area_capacity;

bool
hour_range(int min, int max) {
    time_t now = time(NULL);
    struct tm now_tm;
    localtime_r(&now, &now_tm);
    if (min > max) {
        std::swap(min, max);
    }
    bool inrange = now_tm.tm_hour >= min && now_tm.tm_hour <= max;
    return inrange;
}

char *open_shared_mem(const char *path, int64_t size) {
    void *ptr = MAP_FAILED;
    int fd = -1;
    if ((fd = shm_open(path, O_RDWR | O_CREAT, 0644)) < 0) {
        return 0;
    }
    ftruncate(fd, size);
    ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    return (MAP_FAILED == ptr) ? 0 : static_cast<char *>(ptr);
}

double get_timespec() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}
