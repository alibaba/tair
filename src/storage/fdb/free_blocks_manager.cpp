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
#include "free_blocks_manager.hpp"
#include <stdio.h>
free_blocks_manager::free_blocks_manager(char *pool, int max_block_size)
{
  free_pool = pool;
  free_blocks = (free_block *) (free_pool + sizeof(uint32_t));
  if(max_block_size > 0) {
    set_max_size(max_block_size);
    memset((void *) free_blocks, 0, max_block_size * sizeof(free_block));
  }
  for(size_t i = 0; i < get_max_size(); i++) {
    if(free_blocks[i].size != 0) {
      free_blocks_map.insert(make_pair(free_blocks[i], i));
    }
    else {
      available_pos.push_back(i);
    }
  }

}
uint32_t
free_blocks_manager::get_max_size()  const
{
  return *((uint32_t *) free_pool);
}

void
free_blocks_manager::set_max_size(int max_size)
{
  *((uint32_t *) free_pool) = max_size;
}

void
free_blocks_manager::insert(const free_block & block)
{
  tbsys::CThreadGuard guard(&mutex);
  if(block.size <= 0)
    return;
  if(available_pos.size() == 0) {
    remove_block(free_blocks_map.begin());
  }
  size_t index = available_pos.back();
  available_pos.pop_back();
  free_blocks[index] = block;
  free_blocks_map.insert(make_pair(free_blocks[index], index));
}

bool
free_blocks_manager::search(size_t size, free_block & result)
{
  free_block tmp;
  tmp.size = size;

  tbsys::CThreadGuard guard(&mutex);
  data_type::iterator it = free_blocks_map.lower_bound(tmp);
  if(it == free_blocks_map.end()) {
    return false;
  }
  result = it->first;
  remove_block(it);
  return true;
}

void
free_blocks_manager::remove_block(data_type::iterator it)
{
  free_blocks[it->second].size = 0;
  available_pos.push_back(it->second);
  free_blocks_map.erase(it);
}

////////////////////////////////////////////////////
void
free_blocks_manager::print_pool()  const
{
  for(size_t i = 0; i < get_max_size(); i++) {
    printf("index:%lu;size:%d,pos%lu\n", i, free_blocks[i].size,
           free_blocks[i].offset);
  }
  printf("\n");
}
