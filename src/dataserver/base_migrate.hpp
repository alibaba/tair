/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * migrate interface
 *
 * Version: $Id$
 *
 * Authors:
 *   fanggang <fanggang@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_IMIGRATE_MANAGER_H
#define TAIR_IMIGRATE_MANAGER_H

#include <tbsys.h>
namespace tair {
   class base_migrate{
   public:
      virtual ~base_migrate(){}
      virtual void do_migrate() = 0;
      virtual void do_migrate_one_bucket(int bucket_number, vector<uint64_t> dest_servers) = 0;

   };
}
#endif
