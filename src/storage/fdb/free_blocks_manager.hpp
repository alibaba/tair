/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * free block manager
 *
 * Version: $Id$
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *     - initial release
 *
 */
#include <map>
#include <stdint.h>
#include <vector>
#include <string.h>

#include <tbsys.h>

using namespace std;
typedef struct _fblock
{
  uint32_t size:24;
  uint64_t offset:40;
} free_block;

const static size_t FREEBLOCK_SIZE = sizeof(free_block);
class free_block_comparer {
public:
  bool operator() (const free_block & r1, const free_block & r2) const
  {
    return r1.size < r2.size;
  }
};

class free_blocks_manager {
public:
  explicit free_blocks_manager(char *pool, int max_block_size = 0);
  ~free_blocks_manager()
  {
  };
  uint32_t get_max_size() const;
  void insert(const free_block & block);
  bool search(size_t size, free_block & result);
private:
  typedef multimap<free_block, int, free_block_comparer> data_type;

  void set_max_size(int max_size);
  void remove_block(data_type::iterator it);

  vector <int>available_pos;
  data_type free_blocks_map;

  char *free_pool;
  free_block *free_blocks;
  tbsys::CThreadMutex mutex;
public:
  void print_pool() const;

};
