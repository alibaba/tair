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


using namespace tair;
using namespace std;

void
test()
{
#ifdef TAIR_DEBUG

  TBSYS_LOGGER.setLogLevel("WARN");
  uint16_t area = 0;
  storage::storage_manager * manager = NULL;

  manager = tair::mdb_factory::create_mdb_manager();
  manager->clear(0);
  manager->clear(1);
  manager->clear(2);
  //--------------------test slab balance--------------------------------------------

  //default memsize is 2G
  uint64_t total_size = 2048 * 1024 * 1024UL;
  map<int, uint64_t> quota;
  quota[0] = total_size;
  manager->set_area_quota(quota);

  data_entry pkey;
  data_entry pdata;
  char key[16];
  char data[64];
  char mdata[128];
  int fail_count = 0;
  int bfail_count = 0;


  int count = 1024 * 1024 * 1024 / 128 / 2;
  int bcount = 1024 * 1024 * 1024 / 192 / 2;
  int i = 0;
  memset(data, 'a', sizeof(data));
  memset(mdata, 'a', sizeof(mdata));
  while(i < count) {
    int key_len = sprintf(key, "aa1%011d", i);
    //key[key_len] = '\0';
    pkey.set_data(key, key_len, false);
    pkey.merge_area(area);
    pkey.area = area;
    pdata.set_data(data, 64, false);
    if(manager->put(0, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }
    i++;
  }
  i = 0;
  while(i < bcount) {
    int key_len = sprintf(key, "bb1%011d", i);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(area);
    pkey.area = area;
    pdata.set_data(mdata, 128, false);
    if(manager->put(0, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }
    i++;
  }


  //check page num
  map<int, int> slab_pages1 = ((mdb_manager *) manager)->get_slab_size();


  //mdb_param::m_chkslab_time_low = 0;
  //mdb_param::m_chkslab_time_high = 24;
  // sleep(15);

  //mdb_param::m_chkslab_time_low = 5;
  //mdb_param::m_chkslab_time_high = 7;

  map<int, int> slab_pages2 = ((mdb_manager *) manager)->get_slab_size();
  assert(slab_pages1.size() == slab_pages2.size());
  map<int, int>::const_iterator it1 = slab_pages1.begin();
  map<int, int>::const_iterator it2 = slab_pages2.begin();
  for(; it1 != slab_pages1.end(), it2 != slab_pages2.end(); ++it1, ++it2) {
    assert(it1->first == it2->first);
    assert(it1->second == it2->second);
  }



  int max = 1024 * 1024 * 1024 / 128;
  int bmax = 1024 * 1024 * 1024 / 192;
  while(count < max) {
    int key_len = sprintf(key, "aa2%011d", count);
    pkey.set_data(key, key_len, false);
    pkey.merge_area(area);
    pkey.area = area;
    pdata.set_data(data, 64, false);
    if(manager->put(0, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }
    count++;
  }

  while(bcount < bmax) {
    int keylen = sprintf(key, "bb2%011d", bcount);
    pkey.set_data(key, keylen, false);
    pkey.merge_area(area);
    pkey.area = area;
    pdata.set_data(mdata, 128, false);
    if(manager->put(0, pkey, pdata, true, 0) == 0) {
    }
    else {
      bfail_count++;
    }
    bcount++;
  }


  count = 10 * 10000;
  i = 0;
  while(i < count) {
    int keylen = sprintf(key, "aa3%011d", i);
    pkey.set_data(key, keylen, false);
    pkey.merge_area(area);
    pkey.area = area;
    pdata.set_data(data, 64, false);
    if(manager->put(0, pkey, pdata, true, 0) == 0) {
    }
    else {
      fail_count++;
    }
    i++;
  }

  bcount = 100 * 10000;
  i = 0;
  pdata.set_data(mdata, 128, false);
  while(i < bcount) {
    int keyl_en = sprintf(key, "bb3%011d", i);
    pkey.set_data(key, keyl_en, false);
    pkey.merge_area(area);
    pkey.area = area;
    if(manager->put(0, pkey, pdata, true, 0) == 0) {
    }
    else {
      bfail_count++;
    }
    i++;
  }

  //modify slab balance time
  mdb_param::chkslab_time_low = 0;
  mdb_param::chkslab_time_high = 24;
  sleep(15);

  mdb_param::chkslab_time_low = 5;
  mdb_param::chkslab_time_high = 7;


  sleep(30);
  //check slab page num
  map<int, int> slab_pages3 = ((mdb_manager *) manager)->get_slab_size();
  it2 = slab_pages2.begin();
  map<int, int>::const_iterator it3 = slab_pages3.begin();
  for(; it2 != slab_pages2.end(), it3 != slab_pages3.end(); ++it2, ++it3) {
    printf("-----------------------------------------\n");
    printf("slab id : %d --------------slab id: %d \n", it2->first,
           it3->first);
    assert(it2->second <= it3->second);
  }
#endif
}

int
main(int argc, char *argv[])
{
  test();
  return 0;
}
