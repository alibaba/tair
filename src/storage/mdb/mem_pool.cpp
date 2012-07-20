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
#include "mem_pool.hpp"
#include <sys/mman.h>
#include <assert.h>
#include <string.h>
namespace tair {

#define TEST_BIT(bitmap,x) ((bitmap[x/8] >> (x%8)) & 0x1)
#define SET_BIT(bitmap,x) ((bitmap[x/8] |= (0x1 << (x%8))))
#define CLEAR_BIT(bitmap,x) (bitmap[x/8] &= (~(0x1 << (x%8))))


  using namespace std;

  void mem_pool::initialize(char *pool, int page_size, int total_pages,
                            int meta_len)
  {
    assert(pool != 0);
    assert(page_size >= (1 << 20));
    assert(total_pages > 0);
    assert(static_cast<int>(sizeof(mem_pool_impl)) <=
           mem_pool::MEM_HASH_METADATA_START);

    impl = reinterpret_cast<mem_pool_impl *>(pool);
    impl->initialize(pool, page_size, total_pages, meta_len);
  }

  mem_pool::~mem_pool()
  {
    if (impl->pool != NULL)
    {
      munmap(impl->pool, static_cast<int64_t>(impl->page_size) * impl->total_pages);
    }
  }

  char *mem_pool::alloc_page()
  {
    int index;
    return impl->alloc_page(index);
  }
  char *mem_pool::alloc_page(int &index)
  {
    return impl->alloc_page(index);

  }
  void mem_pool::free_page(const char *page)
  {
    impl->free_page(page);

  }
  void mem_pool::free_page(int index)
  {
    impl->free_page(index);

  }
  char *mem_pool::index_to_page(int index)
  {
    return impl->index_to_page(index);

  }
  int mem_pool::page_to_index(char *page)
  {
    return impl->page_to_index(page);

  }

/*-----------------------------------------------------------------------------
 *  MemPoolImpl
 *-----------------------------------------------------------------------------*/

/* man shm_open:The newly-allocated bytes of a shared memory object are automatically initialised to 0.*/
  void mem_pool::mem_pool_impl::initialize(char *tpool, int this_page_size,
                                           int this_total_pages, int meta_len)
  {
    assert(tpool != 0);
    pool = tpool;
    asm volatile ("mfence":::"memory");
    if(inited != 1) {
      page_size = this_page_size;
      total_pages = this_total_pages;
      memset((void *) &page_bitmap, 0, BITMAP_SIZE);
      int meta_pages = (meta_len + page_size - 1) / page_size;
      for(int i = 0; i < meta_pages; ++i) {
        SET_BIT(page_bitmap, i);
      }
      free_pages = total_pages - meta_pages;
      current_page = meta_pages;
      assert(free_pages > 0);

      inited = 1;
    }
  }
  char *mem_pool::mem_pool_impl::alloc_page(int &index)
  {
    assert(inited == 1);
    if(free_pages == 0) {
      return 0;                        /* there are no pages to allocate */
    }

    /*
     * find a free page index from bitmap
     */
    int page_index;
    for(;; ++current_page) {
      if(current_page == total_pages) {
        current_page = 0;
      }
      if(TEST_BIT(page_bitmap, current_page) == 0) {        /* found */
        page_index = current_page++;
        break;
      }
    }
    index = page_index;
    --free_pages;
    SET_BIT(page_bitmap, index);
    return index_to_page(index);
  }

  void mem_pool::mem_pool_impl::free_page(int index)
  {
    assert(index > 0 && index < total_pages);        /* page 0 is used to META DATA */
    if(TEST_BIT(page_bitmap, index) == 0) {        /* has already released */
      return;
    }
    CLEAR_BIT(page_bitmap, index);
    ++free_pages;
  }

  void mem_pool::mem_pool_impl::free_page(const char *page)
  {
    assert(page != 0);
    int index = page_to_index(page);
    free_page(index);
  }
  char *mem_pool::mem_pool_impl::index_to_page(int index)
  {
    assert(index > 0 && index < total_pages);
    uint64_t offset = static_cast<uint64_t> (index);
    offset *= page_size;
    return pool + offset;
  }
  int mem_pool::mem_pool_impl::page_to_index(const char *page)
  {
    assert(page != 0);
    uint64_t offset = page - pool;
    return offset / static_cast<uint64_t> (page_size);
  }
  char *mem_pool::mem_pool_impl::get_pool_addr()
  {
    return pool;
  }

}                                /* tair */
