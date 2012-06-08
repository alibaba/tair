/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: libmdb_c.hpp 470 2011-12-28 05:26:28Z ganyu.hfl $
 *
 * Authors:
 *   Gan Yu <ganyu.hfl@taobao.com>
 *
 */
#ifndef TAIR_LIBMDB_C_H
#define TAIR_LIBMDB_C_H

#include "mdb_manager.hpp"

#ifdef __cplusplus
extern "C" {
#endif

  typedef void* mdb_t;

  typedef struct {
    const char  *mdb_type; //~ "mdb_shm" for shared memory, "mdb" for heap memory
    const char  *mdb_path; //~ POSIX name of shared memory, like "/mdb_shm_path"
    int64_t     size; //~ size of shm
    int         slab_base_size; //~ the minimal slab size used by mdb
    int         page_size;
    double      factor; //~ the growth factor of slab size
    int         hash_shift; //~ (1 << hash_shift) would be the bucket count of the inner hash map

    int         chkexprd_time_low; //~ lower bound of the expired-checking time range
    int         chkexprd_time_high; //~ upper bound of the expired-checking time range

    int         chkslab_time_low; //~ lower bound of the slab-checking time range
    int         chkslab_time_high; //~ upper bound of the slab-checking time range
  } mdb_param_t;

  //~ kinds of accumulated stat items
  typedef struct {
    uint64_t    quota;
    uint64_t    data_size;
    uint64_t    space_usage;

    uint64_t    item_count;
    uint64_t    hit_count;
    uint64_t    get_count;
    uint64_t    put_count;
    uint64_t    remove_count;
    uint64_t    evict_count;
  } mdb_stat_t;

  typedef struct {
    uint32_t  size;
    char      *data;
  } data_entry_t;

  mdb_t     mdb_init(const mdb_param_t *param);
  void      mdb_destroy(mdb_t db);

  int       mdb_put(mdb_t db, int area, const data_entry_t *key, const data_entry_t *value,
                    int version, bool version_care, int expire);
  int       mdb_get(mdb_t db, int area, const data_entry_t *key, data_entry_t *value, int *version, int *expire);
  int       mdb_del(mdb_t db, int area, const data_entry_t *key, bool version_care);
  int       mdb_add_count(mdb_t db, int area, const data_entry_t *key, int count,
                          int init_value, int expire, int *result);

  int       mdb_clear(mdb_t db, int area); //~ let area be -1 to clear ALL
  void      mdb_get_stat(mdb_t db, int area, mdb_stat_t *stat);
  void      mdb_set_quota(mdb_t db, int area, uint64_t quota);
  uint64_t  mdb_get_quota(mdb_t db, int area);
  bool      mdb_is_quota_exceed(mdb_t db, int area);
  void mdb_assign_static_param(const mdb_param_t *param);

#ifdef __cplusplus
}
#endif

#endif
