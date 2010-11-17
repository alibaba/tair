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
#include <tbsys.h>
#include "mdb_manager.hpp"

using namespace tair;
using namespace
  tair::storage;

void
test_put(mdb_manager & slabcm)
{
  data_entry
    pkey;
  data_entry
    pdata;
  char
    key[128];
  char
    data[65535];

  memset(data, 'A', sizeof(data));
  int
    key_max = 3000000;

  for(int i = 0; i < key_max; i++) {
    int
      key_len =
      sprintf(key, "%08d%d%d%d%d", i, rand(), rand(), rand(), rand());
    pkey.set_data(key, key_len, false);
    pkey.merge_area(0);

    int
      data_len = rand() % 2048 + 1024;
    pdata.set_data(data, data_len, false);

    if(slabcm.put(0, pkey, pdata, false, 0) != 0) {
      fprintf(stderr, "put fail: %s\n", key);
    }
    else {
      fprintf(stderr, "put success: %s\n", key);
    }

    data_entry
      p;
    int
      ret = -1;
    if((ret = slabcm.get(0, pkey, p)) != 0) {
      fprintf(stderr, "get fail:%d\n", ret);
    }

//      if( (ret = slabcm.remove(0,pkey,false)) != 0){
//          fprintf(stderr,"remove fail:%d\n",ret);
//      }
  }
}

void
test_get(mdb_manager & slabcm)
{

  data_entry
    pkey;
  data_entry
    pdata;
  char
    key[128];
  int
    key_max = 10;
  int
    get_max = key_max * 5;


  int
    fail_count = 0;
  for(int i = 0; i < get_max; i++) {
    int
      keylen = sprintf(key, "%08d", i % key_max);
    pkey.set_data(key, keylen, false);
    pkey.merge_area(0);

    if(slabcm.get(0, pkey, pdata) == -1) {
      fail_count++;
      fprintf(stderr, "get fail: %s\n", key);
    }
    else {
      fprintf(stderr, "get success: %s\n", pdata.get_data());
    }
  }
}

void
test_remove(mdb_manager & slabcm)
{

  data_entry
    pkey;
  data_entry
    pdata;
  char
    key[128];
  int
    key_max = 10;
  int
    get_max = key_max * 5;


  int
    fail_count = 0;
  for(int i = 0; i < get_max; i++) {
    int
      keylen = sprintf(key, "%08d", i % key_max);
    pkey.set_data(key, keylen, false);
    pkey.merge_area(0);

    if(slabcm.remove(0, pkey, false) == -1) {
      fprintf(stderr, "remove fail: %s\n", key);
    }
    else {
      fprintf(stderr, "remove success: %s\n", key);
    }

  }
}



int
main(int argc, char **argv)
{

  if(argc < 2) {
    exit(-1);
  }
  int64_t
    size = 2050L * (1L << 20);

  srand(time(NULL));
  mdb_manager
    slabcm;
  slabcm.initialize(size, 1.1, 1048576, "mdb_shm");

  if(argv[1][1] == 'p') {
    test_put(slabcm);
  }
  else if(argv[1][1] == 'g') {
    test_get(slabcm);
  }
  else if(argv[1][1] == 'r') {
    test_remove(slabcm);
  }
  else if(argv[1][1] == 'c') {
    slabcm.clear(0);
  }
  else {
    test_put(slabcm);
    test_get(slabcm);
  }

  return 0;
}
