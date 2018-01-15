/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef __MEM_POOL_H
#define __MEM_POOL_H

#include <stdint.h>
#include <stdio.h>
#include "define.hpp"
#include "mdb_stat.hpp"

namespace tair {

class mem_pool {
public:
    mem_pool(char *pool, int this_page_size, int this_total_pages,
             int meta_len) {
        initialize(pool, this_page_size, this_total_pages, meta_len);
    }

    ~mem_pool();

    char *alloc_page();

    char *alloc_page(int &index);

    void free_page(const char *page);

    void free_page(int index);

    char *index_to_page(int index);

    int page_to_index(char *page);

    char *get_pool_addr() {
        return impl->get_pool_addr();
    }

    int get_page_size() {
        return impl->get_page_size();
    }

    void display_statics() {
        impl->display_statics();
    }

    int get_free_pages_num() {
        return impl->free_pages;
    }

    static const int MAX_PAGES_NO = 65536;
    static const int MDB_VERSION_INFO_START = 12288;  //12k
    static const int MDB_LOCK_INITED_START = MDB_VERSION_INFO_START + 4;
    static const int MDB_LOCK_START = MDB_LOCK_INITED_START + 4;
    static const int MEM_HASH_METADATA_START = 16384;        //16K
    static const int MDB_AREA_TIME_STAMP_START = 32768;        //32K
    static const int MDB_BUCKET_COUNT_START = MDB_AREA_TIME_STAMP_START + sizeof(uint32_t) * TAIR_MAX_AREA_COUNT;
    static const int MDB_BUCKET_TIME_STAMP_START = MDB_BUCKET_COUNT_START + sizeof(uint32_t);
    static const int MEM_POOL_METADATA_LEN = MDB_BUCKET_TIME_STAMP_START + sizeof(uint32_t) * TAIR_MAX_BUCKET_NUMBER;
private:
    void initialize(char *pool, int page_size, int total_pages, int meta_len);

    static const int BITMAP_SIZE = (MAX_PAGES_NO + 7) / 8;

    struct mem_pool_impl {
        void initialize(char *tpool, int page_size, int total_pages,
                        int meta_len);

        char *alloc_page(int &index);

        void free_page(const char *page);

        void free_page(int index);

        char *index_to_page(int index);

        int page_to_index(const char *page);

        char *get_pool_addr();

        int get_page_size() {
            return page_size;
        }

        void display_statics() {
            printf("inited:%d,page_size:%d,total_pages:%d,"
                           "free_pages:%d,current_page:%d\n", inited,
                   page_size, total_pages, free_pages, current_page);
        }

        int inited;
        int page_size;
        int total_pages;
        int free_pages;
        int current_page;
        uint8_t page_bitmap[BITMAP_SIZE];
        char *pool;
    };

    mem_pool_impl *impl;
};

}                                /* tair */
#endif
