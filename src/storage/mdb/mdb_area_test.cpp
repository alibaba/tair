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
 *   FangGang <fanggang@taobao.com>
 *
 */
#include "mdb_factory.hpp"
#include "mdb_manager.hpp"
#include "mdb_define.hpp"
#include "define.hpp"

using namespace tair;
using namespace tair::common;
using namespace std;

void
test()
{
#ifdef TAIR_DEBUG
  TBSYS_LOGGER.setLogLevel("WARN");
  storage::storage_manager * manager = NULL;
  TAIR_STAT.start();

  manager = tair::mdb_factory::create_mdb_manager();
  manager->clear(0);
  manager->clear(1);
  manager->clear(2);

  //--------------------test area quota----------------------------------------------
  //this just use 2 areas
  // insert some data and then modify quota

  data_entry
    pkey;
  data_entry
    pdata;
  char
    key[16];
  char
    data[64];
  char
    mdata[128];
  int
    fail_count = 0;
  int
    bfail_count = 0;
  memset(data, 'a', sizeof(data));
  memset(mdata, 'a', sizeof(mdata));
  map < int,
    uint64_t >
    quota;
  quota[0] = 0;
  quota[1] = 1024 * 1024 * 512UL;
  quota[2] = 1024 * 1024 * 512UL;
  manager->set_area_quota(quota);

  int
    count = 512 * 1024 * 1024 / 128;

  int
    i = 0;
  while(i < count / 2) {
    int
      key_len = sprintf(key, "aa1%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(1);
    pkey.area = 1;
    //pkey.setVersion(0);
    pdata.set_data(data, 64, false);
    if(manager->put(1, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }

    key_len = sprintf(key, "aa2%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(1);
    pkey.area = 1;
    if(manager->put(1, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }

    key_len = sprintf(key, "aa3%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(1);
    pkey.area = 1;
    pdata.set_data(mdata, 128, false);
    if(manager->put(1, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }
    i++;
  }

  int
    bcount = 512 * 1024 * 1024 / 128;
  i = 0;
  while(i < bcount) {
    int
      key_len = sprintf(key, "bb1%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(2);
    pkey.area = 2;
    pdata.set_data(mdata, 128, false);
    if(manager->put(2, pkey, pdata, true, 0) == 0) {
    }
    else {
      bfail_count++;
    }

    key_len = sprintf(key, "bb2%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(2);
    pkey.area = 2;
    pdata.set_data(data, 64, false);
    if(manager->put(2, pkey, pdata, true, 0) == 0) {
    }
    else {
      bfail_count++;
    }
    key_len = sprintf(key, "bb3%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(2);
    pkey.area = 2;
    if(manager->put(2, pkey, pdata, true, 0) == 0) {
    }
    else {
      bfail_count++;
    }

    i += 2;
  }

  //modify quotas
  quota[1] = 1024 * 1024 * 256UL;
  quota[2] = 1024 * 1024 * 256UL;
  manager->set_area_quota(quota);


  mdb_param::chkslab_time_low = 0;
  mdb_param::chkslab_time_high = 24;
  sleep(30);

  mdb_param::chkslab_time_low = 5;
  mdb_param::chkslab_time_high = 7;

  sleep(30);


  vector <int> area_size = ((mdb_manager *) manager)->get_areas();
  for(uint i = 0; i < area_size.size(); i++) {
    if(i == 1) {
      printf("max is %d-------------min is %d------------re is %d",
             1024 * 1024 * 256 + 128, 1024 * 1024 * 256 - 128, area_size[i]);
      assert(1024 * 1024 * 256 - 128 <= area_size[i]);
      assert(1024 * 1024 * 256 + 128 >= area_size[i]);
    }
    else if(i == 2) {
      assert(1024 * 1024 * 256 - 128 <= area_size[i]);
      assert(1024 * 1024 * 256 + 128 >= area_size[i]);

    }
    else {
      assert(area_size[i] == 0);
    }

  }


#endif
}

int
main(int argc, char *argv[])
{
  test();
  return 0;
}
