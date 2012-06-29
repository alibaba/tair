/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: libmdb_test_c.cpp 470 2011-12-28 05:26:28Z ganyu.hfl $
 *
 * Authors:
 *   Gan Yu <ganyu.hfl@taobao.com>
 *
 */

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <tbsys.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <pthread.h>
#include <map>

#include "libmdb_c.hpp"

#define MAX_PROCESS_NUM 50
#define MAX_KEY_SIZE 1024
#define MAX_VALUE_SIZE (1<<22)

mdb_t     init();
void      destroy(mdb_t db);
void      parse_args(int argc, char **argv);
void      test_put(mdb_t db, int id);
void      test_get(mdb_t db, int id);
void      test_del(mdb_t db, int id);
void      test_lookup(mdb_t db, int id);
void      disp_stat(mdb_stat_t *stat);
char      *genk(int size, uint64_t seq, bool rand);
char      *genv(int size);
void      sig_chld_handler(int sig);

typedef struct {
  pid_t       pid;
  uint64_t    key_start;
  uint64_t    key_end;
} proc_param_t;

typedef struct {
  uint32_t    nproc;
  int         area;
  uint64_t    quota;
  uint32_t    item_count;
  uint32_t    key_size;
  uint32_t    max_value_size;
  uint32_t    min_value_size;
  uint64_t    mem_size;
  const char  *mem_type;
  const char  *shm_path;
  const char  *action_type; //~ put, get, delete, etc.
  const char  *action_mode; //~ seq, random
  const char  *log; //~ if NULL, stderr is used
  const char  *prog;
} args_t;

typedef struct {
  uint64_t nsuccess;
  uint64_t nfail;
} result_t;

static proc_param_t proc_params[MAX_PROCESS_NUM];
static args_t args;
static char   *kbuf = NULL;
static char   *vbuf = NULL;
static int    mod = '~' - ' ' + 1;
static pthread_mutex_t *mtx = NULL;
static uint32_t nproc_done = 0;

int
main(int argc, char **argv) {
  parse_args(argc, argv);
  signal(SIGCHLD, sig_chld_handler);

  mdb_t db = init();
  if (db == NULL) {
    fprintf(stderr, "mdb init failed\n");
    exit(1);
  }
  mdb_set_quota(db, args.area, args.quota);
  if (args.item_count < args.nproc) {
    fprintf(stderr, "item_count less than nproc");
    destroy(db);
    exit(1);
  }
  uint64_t seq = 0LL;
  uint64_t step = args.item_count/args.nproc;
  for (size_t i = 0; i < args.nproc; ++i) {
    //~ prep
    uint64_t start = seq;
    uint64_t end = seq + step;
    seq += step;
    end = end > args.item_count? args.item_count : end;
    proc_params[i].key_start = start;
    proc_params[i].key_end = end;
    pid_t pid;
    if ((pid = fork()) == 0) {
      //~ children
      mdb_log_level("warn");
      if (args.log != NULL) {
        mdb_log_file(args.log);
      }
      srand(time(NULL) + getpid());
      if (strcmp(args.action_type, "put") == 0) {
        test_put(db, i);
      } else if (strcmp(args.action_type, "get") == 0) {
        test_get(db, i);
      } else if (strcmp(args.action_type, "del") == 0) {
        test_del(db, i);
      } else if (strcmp(args.action_type, "lookup") == 0) {
        test_lookup(db, i);
      } else {
        fprintf(stderr, "action type %s not supported\n", args.action_type);
      }
      fprintf(stderr, "exit(%lu)\n", i);
      exit(i);
    } else if (pid > 0) {
      //~ parent
      proc_params[i].pid = pid;
    } else {
      perror("fork");
      exit(1);
    }
  }

  mdb_stat_t stat;
  while (nproc_done < args.nproc) {
    mdb_get_stat(db, args.area, &stat);
    disp_stat(&stat);
    sleep(2);
  }
  mdb_get_stat(db, args.area, &stat);
  disp_stat(&stat);

  destroy(db);
  return 0;
}

mdb_t init() {
  kbuf = new char[MAX_KEY_SIZE];
  vbuf = new char[MAX_VALUE_SIZE];
  memset(kbuf, 0, MAX_KEY_SIZE);
  memset(vbuf, 0, MAX_VALUE_SIZE);

  mtx = (pthread_mutex_t*)mmap(NULL, sizeof(pthread_mutex_t), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
  if (mtx == MAP_FAILED) {
    perror("mmap");
    return NULL;
  } else {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(mtx, &attr);
  }

  mdb_param_t params;
  memset(&params, 0, sizeof(mdb_param_t));
  params.mdb_type = args.mem_type;
  params.mdb_path = args.shm_path;
  params.size = args.mem_size;
  mdb_t db = mdb_init(&params);
  return db;
}

void destroy(mdb_t db) {
  delete [] kbuf;
  delete [] vbuf;
  pthread_mutex_destroy(mtx);
  munmap(mtx, sizeof(pthread_mutex_t));
  mdb_destroy(db);
}

void parse_args(int argc, char **argv) {
  args.nproc = 2;
  args.area = 3;
  args.mem_size = 1LL<<30;
  //args.quota = args.mem_size>>2;
  args.quota = 0;
  args.item_count = 1<<16;
  args.key_size = 16;
  args.max_value_size = 64;
  args.min_value_size = 64;
  args.mem_type = "mdb_shm";
  args.shm_path = "/libmdb_shm";
  args.action_type = "put";
  args.action_mode = "rand";
  args.log = NULL;
  args.prog = argv[0];

  int ret = 0;
  while((ret = getopt(argc, argv, "n:p:s:k:v:l:q:t:m:a:c:r:h")) != -1) {
    switch (ret) {
    case 'n':
      args.nproc = atoi(optarg);
      if (args.nproc > MAX_PROCESS_NUM) {
        args.nproc = MAX_PROCESS_NUM;
      }
      break;
    case 'p':
      args.shm_path = optarg;
      break;
    case 's':
      args.mem_size = atoll(optarg);
      break;
    case 'm':
      args.mem_type = optarg;
      break;
    case 't':
      args.action_type = optarg;
      break;
    case 'l':
      args.log = optarg;
      break;
    case 'k':
      args.key_size = atoi(optarg);
      break;
    case 'v':
      sscanf(optarg, "%u-%u", &args.min_value_size, &args.max_value_size);
      if (args.max_value_size < args.min_value_size) {
        std::swap(args.max_value_size, args.min_value_size);
      }
      break;
    case 'q':
      args.quota = atoll(optarg);
      break;
    case 'c':
      args.item_count = atoi(optarg);
      break;
    case 'r':
      args.action_mode = optarg;
      break;
    case 'h':
    default:
      //usage(argv[0]);
      exit(0);
    };
  }
}

char *genk(int size, uint64_t seq, bool is_rand) {
  if (is_rand) {
    for (int i = 0; i < size; ++i) {
      kbuf[i] = rand()%mod + ' ';
    }
  } else {
    int n = 0;
    while (n < size-1) {
      n += snprintf(kbuf + n, size - n, "%"PRIX64, seq);
    }
  }
  return kbuf;
}

char *genv(int size) {
  static std::map<int, char*> vmap; //~ normally not too large, won't free
  std::map<int, char*>::iterator it = vmap.find(size);
  char *value = NULL;
  if (it != vmap.end()) {
    value = (char*)malloc(size);
    //for (int i = 0; i < size; ++i) {
      //value[i] = rand()%mod + ' ';
    //}
    //value[size-1] = 0;
    vmap.insert(std::make_pair(size, value));
  } else {
    value = it->second;
  }
  return value;
}

void test_put(mdb_t db, int id) {
  bool random = true;
  if (strcmp(args.action_mode, "seq") == 0) {
    random  = false;
  }
  data_entry_t key;
  data_entry_t value;
  result_t put_result = {0LL,0LL};
  time_t s = time(NULL);
  fprintf(stderr, "start: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "key_start: %"PRIu64", key_end: %"PRIu64"\n", proc_params[id].key_start, proc_params[id].key_end);
  uint32_t range = args.max_value_size - args.min_value_size;
  if (range == 0) range = 1;
  for (size_t i = proc_params[id].key_start; i < proc_params[id].key_end; ++i) {
    key.data = genk(args.key_size, i, random);
    key.size = args.key_size;
    uint32_t value_size = args.min_value_size + (i % range);
    value.data = genv(value_size);
    value.size = value_size;

    //pthread_mutex_lock(mtx);
    int rc = mdb_put(db, args.area, &key, &value, 0, 1, 0);
    //pthread_mutex_unlock(mtx);
    if (rc == 0) {
      ++put_result.nsuccess;
    } else {
      ++put_result.nfail;
    }
  }
  time_t e = time(NULL);
  fprintf(stderr, "end: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "avg qps: %lu, time consumed: %ld\n", (proc_params[id].key_end-proc_params[id].key_start)/(e-s), e - s);
}
void test_get(mdb_t db, int id) {
  data_entry_t key;
  data_entry_t value;
  result_t get_result = {0LL,0LL};
  time_t s = time(NULL);
  fprintf(stderr, "start: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "key_start: %"PRIu64", key_end: %"PRIu64"\n", proc_params[id].key_start, proc_params[id].key_end);
  for (size_t i = proc_params[id].key_start; i < proc_params[id].key_end; ++i) {
    key.data = genk(args.key_size, i, false);
    key.size = args.key_size;
    value.data = NULL;
    value.size = 0;
    pthread_mutex_lock(mtx);
    int rc = mdb_get(db, args.area, &key, &value, NULL, NULL);
    pthread_mutex_unlock(mtx);
    if (rc == 0) {
      ++get_result.nsuccess;
      free(value.data);
    } else {
      ++get_result.nfail;
    }
  }
  time_t e = time(NULL);
  fprintf(stderr, "end: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "avg qps: %lu, time consumed: %ld\n", (proc_params[id].key_end-proc_params[id].key_start)/(e-s), e - s);
}

void test_del(mdb_t db, int id) {
  data_entry_t key;
  result_t del_result = {0LL,0LL};
  time_t s = time(NULL);
  fprintf(stderr, "start: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "key_start: %"PRIu64", key_end: %"PRIu64"\n", proc_params[id].key_start, proc_params[id].key_end);
  for (size_t i = proc_params[id].key_start; i < proc_params[id].key_end; ++i) {
    key.data = genk(args.key_size, i, false);
    key.size = args.key_size;
    pthread_mutex_lock(mtx);
    int rc = mdb_del(db, args.area, &key, false);
    pthread_mutex_unlock(mtx);
    if (rc == 0) {
      ++del_result.nsuccess;
    } else {
      ++del_result.nfail;
    }
  }
  time_t e = time(NULL);
  fprintf(stderr, "end: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "avg qps: %lu, time consumed: %ld\n", (proc_params[id].key_end-proc_params[id].key_start)/(e-s), e - s);
}

void test_lookup(mdb_t db, int id) {
  data_entry_t key;
  result_t lookup_result = {0LL,0LL};
  time_t s = time(NULL);
  fprintf(stderr, "start: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "key_start: %"PRIu64", key_end: %"PRIu64"\n", proc_params[id].key_start, proc_params[id].key_end);
  for (size_t i = proc_params[id].key_start; i < proc_params[id].key_end; ++i) {
    key.data = genk(args.key_size, i, false);
    key.size = args.key_size;
    pthread_mutex_lock(mtx);
    bool rc = mdb_lookup(db, args.area, &key);
    pthread_mutex_unlock(mtx);
    if (rc == true) {
      ++lookup_result.nsuccess;
    } else {
      ++lookup_result.nfail;
    }
  }
  time_t e = time(NULL);
  fprintf(stderr, "end: process %d, pid: %d\n", id, getpid());
  fprintf(stderr, "avg qps: %lu, time consumed: %ld, exist: %lu, nonexist: %lu\n",
      (proc_params[id].key_end-proc_params[id].key_start)/(e-s), e - s, lookup_result.nsuccess, lookup_result.nfail);
}

void disp_stat(mdb_stat_t *stat) {
  fprintf(stderr,
      "use_size: %"PRIu64",\n"
      "data_size: %"PRIu64",\n"
      "item_count: %"PRIu64",\n"
      "put_count: %"PRIu64",\n"
      "get_count: %"PRIu64",\n"
      "hit_count: %"PRIu64",\n"
      "remove_count: %"PRIu64",\n"
      "evict_count: %"PRIu64",\n",
      stat->space_usage,
      stat->data_size,
      stat->item_count,
      stat->put_count,
      stat->get_count,
      stat->hit_count,
      stat->remove_count,
      stat->evict_count);
}

void sig_chld_handler(int sig) {
  pid_t pid;
  int status;
  if ((pid = wait(&status)) < 0) {
    perror("wait");
  } else {
    ++nproc_done;
    fprintf(stderr, "process %d, pid %d, finished\n", WEXITSTATUS(status), pid);
  }
}
