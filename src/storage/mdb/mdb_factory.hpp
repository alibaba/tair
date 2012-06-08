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
#ifndef TAIR_STORAGE_MDB_FACTORY_H
#define TAIR_STORAGE_MDB_FACTORY_H

#include "storage/storage_manager.hpp"
namespace tair {

  class mdb_factory {
  public:
    static storage::storage_manager * create_mdb_manager();
    static storage::storage_manager * create_embedded_mdb();
  };

}                                /* tair */

#endif
