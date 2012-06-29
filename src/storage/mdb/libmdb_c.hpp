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

  /*
   * init one mdb instance with `param',
   * if you don't care specific parameter, let it be 0/NULL
   */
  mdb_t     mdb_init(const mdb_param_t *param);
  /*
   * destroy the mdb
   */
  void      mdb_destroy(mdb_t db);
  /*
   * put
   * @param db: mdb instance into which you insert the entry
   * @param area: also called namespace, used to isolate keys
   * @param version: version of the entry, maintained by mdb, increased by one every update
   * @param version_care: whether you care about the version
   * @param expire: lifetime of the entry, by seconds, absolute or relative
   */
  int       mdb_put(mdb_t db, int area, const data_entry_t *key, const data_entry_t *value,
                    int version, bool version_care, int expire);
  /*
   * get
   * @param version: if not NULL, version of the entry would be got
   * @param expire: if not NULL, expire of the entry would be got
   */
  int       mdb_get(mdb_t db, int area, const data_entry_t *key, data_entry_t *value, int *version, int *expire);
  /*
   * delete
   */
  int       mdb_del(mdb_t db, int area, const data_entry_t *key, bool version_care);
  /*
   * counter
   * @param count: increment
   * @param init_value: base value if not existing
   * @param result: result of the incrementation
   */
  int       mdb_add_count(mdb_t db, int area, const data_entry_t *key, int count,
                          int init_value, int expire, int *result);
  /*
   * test existence
   */
  bool      mdb_lookup(mdb_t db, int area, const data_entry_t *key);
  /*
   * clear the whole entries of one area
   * if area equals -1, every area would be cleared
   */
  int       mdb_clear(mdb_t db, int area); //~ let area be -1 to clear ALL
  /*
   * get the statistics of specific area
   */
  void      mdb_get_stat(mdb_t db, int area, mdb_stat_t *stat);
  /*
   * set the upper space the area could hold
   */
  void      mdb_set_quota(mdb_t db, int area, uint64_t quota);
  /*
   * get the quota of specifig area
   */
  uint64_t  mdb_get_quota(mdb_t db, int area);
  /*
   * see if one area's occupation exceeds its quota
   */
  bool      mdb_is_quota_exceed(mdb_t db, int area);
  /*
   * set log file
   */
  void      mdb_log_file(const char *log);
  /*
   * set log level
   * @param level: 'error', 'warn', 'info', 'debug', case insensitive
   */
  void      mdb_log_level(const char *level);
#ifdef __cplusplus
}
#endif

#endif
