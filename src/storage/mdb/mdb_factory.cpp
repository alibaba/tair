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
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include "mdb_factory.hpp"
#include "mdb_manager.hpp"
#include "mdb_define.hpp"
#include "define.hpp"
#include "tbsys.h"
namespace tair {
  storage::storage_manager * mdb_factory::create_mdb_manager()
  {
    bool use_share_mem = true;
    const char* use_mem_type = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MDB_TYPE, "mdb_shm");
    if (strcmp(use_mem_type, "mdb_shm") == 0)
    {
      use_share_mem = true;
    }
    else if (strcmp(use_mem_type, "mdb") == 0)
    {
      use_share_mem = false;
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid mdb use memory type: %s. only support mdb_shm or mdb.", use_mem_type);
      return NULL;
    }

    mdb_param::mdb_path =
      TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MDB_SHM_PATH,
                             "/mdb_shm_path");
    mdb_param::size =
      TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_SLAB_MEM_SIZE, 2048);
    mdb_param::slab_base_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,TAIR_SLAB_BASE_SIZE,64);

    mdb_param::page_size =
      TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_SLAB_PAGE_SIZE, 1048576);
    mdb_param::factor =
      (TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_SLAB_FACTOR, 110) * 1.0) / 100;

    mdb_param::hash_shift =
      TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_MDB_HASH_BUCKET_SHIFT, 23);

    const char *hour_range =
      TBSYS_CONFIG.getString(TAIRSERVER_SECTION,
                             TAIR_CHECK_EXPIRED_HOUR_RANGE, "2-4");
    assert(hour_range != 0);
    if(sscanf
       (hour_range, "%d-%d", &mdb_param::chkexprd_time_low,
        &mdb_param::chkexprd_time_high) != 2) {
      mdb_param::chkexprd_time_low = 2;
      mdb_param::chkexprd_time_high = 4;
    }

    hour_range =
      TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_CHECK_SLAB_HOUR_RANGE,
                             "5-7");
    assert(hour_range != 0);
    if(sscanf
       (hour_range, "%d-%d", &mdb_param::chkslab_time_low,
        &mdb_param::chkslab_time_high) != 2) {
      mdb_param::chkslab_time_low = 5;
      mdb_param::chkslab_time_high = 7;
    }

    mdb_param::size *= (1 << 20);        //in MB

    assert(mdb_param::size > 0);
    assert(mdb_param::page_size > 0);

    assert((mdb_param::factor - 1.0) > 0.0);

    TBSYS_LOG(DEBUG, "size:%lu,page_size:%d,m_factor:%f,m_hash_shift:%d",
              mdb_param::size, mdb_param::page_size, mdb_param::factor,
              mdb_param::hash_shift);

    storage::storage_manager * manager = 0;

    manager = new mdb_manager();
    if(manager != 0) {
      if(dynamic_cast<mdb_manager *>(manager)->initialize(use_share_mem)) {
        return manager;
      }
    }
    //FAIL
    if(manager != 0)
      delete manager;
    return 0;
  }

  storage::storage_manager * mdb_factory::create_embedded_mdb()
  {
    storage::storage_manager * manager = create_mdb_manager();
    if (manager == 0){
            return 0;
    }
    for(int i=0; i<TAIR_MAX_AREA_COUNT;++i) {
            manager->set_area_quota(i,0); // default clear all area quota, user should set area quota themselves
    }

    log_warn("create embedded mdb, mem type: %s, cache size: %"PRI64_PREFIX"d, page size: %d, factor: %f, hash_shift: %d.",
             TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MDB_TYPE, "mdb_shm"),
             mdb_param::size, mdb_param::page_size, mdb_param::factor, mdb_param::hash_shift);
    return manager;

  }

}

/* tair */
