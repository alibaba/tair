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
#include <iostream>
#include <pthread.h>
#include <time.h>
#include <tbsys.h>
#include "mdb_factory.hpp"
#include "mdb_manager.hpp"
#include "mdb_define.hpp"
#include "define.hpp"


using namespace tair;
using namespace std;

struct thread_arg
{
  int index;
  int area;
  pthread_t id;
    storage::storage_manager * cm;
  int key_base;
  int max_loop;
  int min_data_size;
  int max_data_size;
  int items_between_each_report;
  volatile int success;
  volatile int failed;
  volatile time_t total_time;
};

void
usage(const char *prog)
{
  fprintf(stderr, "%s       -t type[mdb,mdb_shm],default is mdb_shm\n"
          "       \t\t-p threadnum,default is 1\n"
          "       \t\t-s size range(e.g. 100-200,default is 1-1024(bytes))\n"
          "       \t\t-m size,how many items do you want to write\n"
          "       \t\t-i interval,the amount of items between each report,default is 100000(10W)\n"
          "       \t\t-l size of mdb[unit: M]\n"
          "       \t\t-o operation(p(put),g(get),r(remove))\n"
          "       \t\t-h print this message\n", prog);
}

void *
put_thread(void *arg)
{
  thread_arg *ta = (thread_arg *) arg;
  storage::storage_manager * slabcm = ta->cm;

  data_entry pkey;
  data_entry pdata;
  char key[16];
  char data[65536];
  int64_t total_len = 0;

  srand(time(NULL));

  int report_per_times = ta->items_between_each_report;

  memset(data, 'A', sizeof(data));

  for(int i = 0; i < ta->max_loop; i += report_per_times) {
    time_t start_time = time(NULL);

    for(int j = 0; j < report_per_times; ++j) {
      int key_len = sprintf(key, "key%011d", i + j + ta->key_base);
      pkey.set_data(key, key_len, false);
      pkey.merge_area(ta->area);
      pkey.area = ta->area;

      int data_len = rand() % ta->max_data_size + ta->min_data_size;
      pdata.set_data(data, data_len, false);

      if(slabcm->put(0, pkey, pdata, true, 0) == 0) {
        ta->success += 1;
        total_len += (key_len + data_len);
      }
      else {
        ta->failed += 1;
      }
    }

    time_t end_time = time(NULL);

    ta->total_time += end_time - start_time;

    TBSYS_LOG(WARN,
              "[%d]in %d seconds,success:%d,failed:%d,total data : %ld bytes",
              ta->index, ta->total_time, ta->success, ta->failed, total_len);
  }
  return NULL;
}


void *
get_thread(void *arg)
{
  thread_arg *ta = (thread_arg *) arg;
  storage::storage_manager * slabcm = ta->cm;

  data_entry pkey;
  data_entry pdata;
  char key[16];
  int64_t total_len;


  int report_per_times = ta->items_between_each_report;

  for(int i = 0; i < ta->max_loop; i += report_per_times) {
    time_t start_time = time(NULL);

    for(int j = 0; j < report_per_times; ++j) {
      int key_len = sprintf(key, "key%011d", i + j + ta->key_base);
      pkey.set_data(key, key_len, false);
      pkey.merge_area(ta->area);
      pkey.area = ta->area;

      if(slabcm->get(0, pkey, pdata) == 0) {
        ta->success += 1;
        total_len += (key_len + pdata.get_size());
      }
      else {
        ta->failed += 1;
      }
    }

    time_t end_time = time(NULL);

    ta->total_time += end_time - start_time;

    TBSYS_LOG(WARN,
              "[%d]in %d seconds,success:%d,failed:%d,total data : %ld bytes",
              ta->index, ta->total_time, ta->success, ta->failed, total_len);
  }
  return NULL;
}

void *
remove_thread(void *arg)
{
  thread_arg *ta = (thread_arg *) arg;
  storage::storage_manager * slabcm = ta->cm;

  data_entry pkey;
  char key[16];

  int report_per_times = ta->items_between_each_report;

  for(int i = 0; i < ta->max_loop; i += report_per_times) {
    time_t start_time = time(NULL);

    for(int j = 0; j < report_per_times; ++j) {
      int key_len = sprintf(key, "key%011d", i + j + ta->key_base);
      pkey.set_data(key, key_len, false);
      pkey.merge_area(ta->area);
      pkey.area = ta->area;

      if(slabcm->remove(0, pkey, false) == 0) {
        ta->success += 1;
      }
      else {
        ta->failed += 1;
      }
    }

    time_t end_time = time(NULL);

    ta->total_time += end_time - start_time;

    TBSYS_LOG(WARN, "[%d]in %d seconds,success:%d,failed:%d", ta->index,
              ta->total_time, ta->success, ta->failed);
  }
  return NULL;
}

int
main(int argc, char *argv[])
{
  int64_t size = 2050L * (1L << 20);
  int thread_count = 1;
  int max_loop = 500000;
  int min_data_size = 1;
  int max_data_size = 1024;
  int report_interval = 100000;
  char *mdb_type = "mdb_shm";
  int key_base = 0;
  char *operation = 0;
  int area = 0;

  TBSYS_LOGGER.setLogLevel("WARN");

  int ret = 0;
  while((ret = getopt(argc, argv, "t:p:s:m:i:l:hk:o:n:")) != -1) {
    switch (ret) {
    case 't':
      mdb_type = optarg;
      break;
    case 'p':
      thread_count = atoi(optarg);
      break;
    case 's':
      sscanf(optarg, "%d-%d", &min_data_size, &max_data_size);
      break;
    case 'm':
      max_loop = atoi(optarg);
      break;
    case 'i':
      report_interval = atoi(optarg);
      break;
    case 'l':
      size = atoi(optarg) * (1L << 20);
      break;
    case 'k':
      key_base = atoi(optarg);
      break;
    case 'o':
      operation = optarg;
      break;
    case 'n':
      area = atoi(optarg);
      break;
    case 'h':
    default:
      usage(argv[0]);
      exit(0);
    };
  }

  if(optind != argc || operation == 0) {
    usage(argv[0]);
    exit(-1);
  }

  TBSYS_LOG(WARN,
            "thread_count : %d,loop times per thread : %d,min_data_size : %d,max_data_size : %d,report_interval : %d,mdb size : %dM",
            thread_count, max_loop, min_data_size, max_data_size,
            report_interval, size / (1L << 20));

  storage::storage_manager * manager = 0;

  //mdb_param::m_factor = 1.1;
  //mdb_param::m_size = size;
  //mdb_param::m_page_size = 1048576;

  //manager = mdb_factory::create_mdb_manager();
  manager = mdb_factory::create_mdb_manager();

  if(*operation == 's') {
    sleep(600);
    return 0;
  }
  else if(*operation == 'c') {
    manager->clear(area);
    return 0;
  }

  for(int i = 0; i < 30; ++i) {
    manager->set_area_quota(i, 3145728000);
  }

  thread_arg *ta[thread_count];

  for(int i = 0; i < thread_count; ++i) {
    ta[i] = new thread_arg;
    ta[i]->index = i;
    ta[i]->area = area;
    ta[i]->cm = manager;
    ta[i]->max_loop = max_loop;
    ta[i]->min_data_size = min_data_size;
    ta[i]->max_data_size = max_data_size;
    ta[i]->items_between_each_report = report_interval;
    ta[i]->key_base = ta[i]->max_loop * i + key_base;
    ta[i]->success = ta[i]->failed = 0;
    ta[i]->total_time = 0;
    ret =
      pthread_create(&(ta[i]->id), NULL,
                     *operation == 'p' ? put_thread : (*operation ==
                                                       'g' ? get_thread :
                                                       remove_thread),
                     (void *) ta[i]);
    assert(ret == 0);
  }

  for(int i = 0; i < thread_count; ++i) {
    pthread_join(ta[i]->id, NULL);
    delete ta[i];
  }

  //sleep(30);

  return 0;
}
