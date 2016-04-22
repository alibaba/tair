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

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <getopt.h>
#include <fcntl.h>
#include <assert.h>

#include <tbsys.h>

#include "mdb_manager.hpp"

using namespace tair;

typedef struct param_t param_t;

struct param_t {
    bool is_area_stat;
    bool interactive;
    bool verbose;
    const char *shm_file;
    const char *out_file;
};

typedef struct page_pool_t page_pool_t;
struct page_pool_t {
    int inited;
    int page_size;
    int total_pages;
    int free_pages;
    int current_page;
    uint8_t page_bitmap[(mem_pool::MAX_PAGES_NO + 7) / 8];
    char *pool;
};

typedef struct cache_info_t cache_info_t;
struct cache_info_t {
    int inited;
    int max_slab_id;
    int base_size;
    float factor;
};

typedef struct slab_t slab_t;
struct slab_t {
    mem_pool *pool;
    mem_cache *cache;
    uint32_t slab_id;
    int slab_size;
    int per_slab;
    int page_size;

    struct item_list {
        uint64_t item_head;
        uint64_t item_tail;
    };

    item_list this_item_list[TAIR_MAX_AREA_COUNT];
    uint64_t item_count[TAIR_MAX_AREA_COUNT];
    uint64_t evict_count[TAIR_MAX_AREA_COUNT];

    uint64_t item_total_count;
    uint64_t evict_total_count;

    int full_pages_no;
    int partial_pages_no;
    int free_pages_no;
    int evict_index;

    uint32_t free_pages;
    uint32_t full_pages;
    uint32_t *partial_pages;
    int partial_pages_bucket_num;
    int first_partial_page_index;
    uint32_t partial_bucket_count;
};

typedef struct hash_t hash_t;
struct hash_t {
    int inited;
    int bucket;
    int item_count;
    int start_page;
};

static char *open_shm(const char *path);

static void parse_area_stat(param_t *param);

static void parse_mem_stat(param_t *param);

static page_pool_t *pool_info(char *base);

static uint32_t bucket_count(char *base);

static cache_info_t *cache_info(char *base);

static slab_t *slab_start(char *base);

static slab_t *slab_next(slab_t *slab);

static hash_t *hash_info(char *base);

static mdb_item *id2item(char *pool, int page_size, uint64_t id, slab_t **slabs);

static int items_of_bucket(char *pool, uint64_t *ht, int bucket, slab_t **slabs);

static void help(void);

static bool parse_params(int argc, char **argv, param_t *param);

static FILE *out;

int
main(int argc, char **args) {
    param_t param;
    param.shm_file = NULL;
    param.out_file = NULL;
    param.is_area_stat = false;
    param.interactive = false;
    param.verbose = false;

    if (!parse_params(argc, args, &param)) {
        exit(1);
    }

    out = stderr;
    if (param.out_file != NULL && (out = fopen(param.out_file, "w")) == NULL) {
        fprintf(stderr, "open %s: %m\n", param.out_file);
        out = stderr;
    }

    if (param.is_area_stat) {
        parse_area_stat(&param);
    } else {
        parse_mem_stat(&param);
    }

    fclose(out);
    return 0;
}

void parse_area_stat(param_t *param) {
    mdb_area_stat *area_stat = reinterpret_cast<mdb_area_stat *> (open_shm(param->shm_file));
    if (area_stat == NULL) {
        fprintf(stderr, "open %s failed: %m\n", param->shm_file);
        exit(1);
    }
    fprintf(out,
            "%5s"
                    "%16s"
                    "%16s"
                    "%16s"
                    "%16s"
                    "%16s"
                    "%16s"
                    "%16s"
                    "%16s"
                    "%16s",
            "area",
            "get",
            "hit",
            "put",
            "remove",
            "evict",
            "items",
            "usage",
            "size",
            "quota\n");
    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        if (!param->verbose && area_stat[i].get_count
                               + area_stat[i].hit_count
                               + area_stat[i].put_count
                               + area_stat[i].remove_count
                               + area_stat[i].evict_count
                               + area_stat[i].item_count
                               + area_stat[i].space_usage
                               + area_stat[i].data_size == 0) {
            continue;
        }
        fprintf(out,
                "%5d"
                        "%16lu"
                        "%16lu"
                        "%16lu"
                        "%16lu"
                        "%16lu"
                        "%16lu"
                        "%16lu"
                        "%16lu"
                        "%16lu\n",
                i,
                area_stat[i].get_count,
                area_stat[i].hit_count,
                area_stat[i].put_count,
                area_stat[i].remove_count,
                area_stat[i].evict_count,
                area_stat[i].item_count,
                area_stat[i].space_usage,
                area_stat[i].data_size,
                area_stat[i].quota);
    }
}

void parse_mem_stat(param_t *param) {
    char *addr = open_shm(param->shm_file);
    if (addr == NULL) {
        fprintf(stderr, "open %s failed: %m\n", param->shm_file);
        exit(1);
    }
    /**
     * page pool info
     */
    page_pool_t *pool = pool_info(addr);
    fprintf(out, "page info:\n");
    fprintf(out, "\tinited: %d, page_size: %d, total_pages: %d\n", pool->inited, pool->page_size, pool->total_pages);
    fprintf(out, "\tfree_pages: %d, current_pages: %d, bucket_count: %u\n", pool->free_pages, pool->current_page,
            bucket_count(addr));
    if (param->verbose) {
        fprintf(out, "==================================bitmap=====================================");
        for (uint32_t i = 0; i < sizeof(pool->page_bitmap); ++i) {
            if (!(i & 0xF)) {
                fprintf(out, "\n");
            }
            int8_t byte = pool->page_bitmap[i];
            for (int j = 0; j < 8; ++j, byte <<= 1) {
                fprintf(out, "%d", (byte & 0x80) != 0);
            }
            fprintf(out, "|");
        }
        fprintf(out, "\n");
    }
    /**
     * cache info
     */
    cache_info_t *cache = cache_info(addr);
    fprintf(out, "cache info:\n");
    fprintf(out, "\tinited: %d, max_slab_id: %d, base_size: %d, factor: %f\n",
            cache->inited, cache->max_slab_id, cache->base_size, cache->factor);
    /**
     * slab info
     */
    slab_t *slabs[cache->max_slab_id + 1];
    slab_t *slab = slab_start(addr);
    fprintf(out,
            "%4s"
                    "%9s"
                    "%9s"
                    "%15s"
                    "%15s"
                    "%9s"
                    "%9s"
                    "%9s"
                    "%9s"
                    "%9s"
                    "%9s\n",
            "slab",
            "size",
            "per_slab",
            "items",
            "evicts",
            "pgfull",
            "pgfree",
            "pgpart",
            "part_1st",
            "n_bucket",
            "n_part");

    for (int i = 0; i <= cache->max_slab_id; ++i) {
        fprintf(out, "%4u%9d%9d%15lu%15lu%9d%9d%9d%9d%9d%9u\n",
                slab->slab_id,
                slab->slab_size,
                slab->per_slab,
                slab->item_total_count,
                slab->evict_total_count,
                slab->full_pages_no,
                slab->free_pages_no,
                slab->partial_pages_no,
                slab->first_partial_page_index,
                slab->partial_pages_bucket_num,
                slab->partial_bucket_count);
        slabs[slab->slab_id] = slab;
        slab = slab_next(slab);
    }
    /**
     * hash info
     */
    hash_t *meta = hash_info(addr);
    uint64_t *ht = reinterpret_cast<uint64_t *> (addr + meta->start_page * mdb_param::page_size);
    fprintf(out, "hash info:\n");
    fprintf(out, "\tinited: %d, bucket_count, %d, item_count: %d, start_page: %d\n",
            meta->inited, meta->bucket, meta->item_count, meta->start_page);
    int items_dist[256] = {0};
    for (int i = 0; i < meta->bucket; ++i) {
        uint32_t items = items_of_bucket(addr, ht, i, slabs);
        if (items >= (sizeof(items_dist) >> 2)) {
            fprintf(out, "bucket %d holds %d items!\n", i, items);
            continue;
        }
        ++items_dist[items];
    }
    fprintf(out, "%12s%12s\n", "item_count", "buckt_count");
    for (uint32_t i = 0; i < (sizeof(items_dist) >> 2); ++i) {
        if (items_dist[i] != 0) {
            fprintf(out, "%12d%12d\n", i, items_dist[i]);
        }
    }
}

page_pool_t *
pool_info(char *addr) {
    return reinterpret_cast<page_pool_t *> (addr);
}

static uint32_t
bucket_count(char *addr) {
    return *reinterpret_cast<uint32_t *> (addr + mem_pool::MDB_BUCKET_COUNT_START);
}

static cache_info_t *
cache_info(char *addr) {
    return reinterpret_cast<cache_info_t *> (addr + mem_pool::MEM_POOL_METADATA_LEN);
}

static slab_t *
slab_start(char *addr) {
    return reinterpret_cast<slab_t *> (addr + mem_pool::MEM_POOL_METADATA_LEN + sizeof(cache_info_t));
}

static slab_t *
slab_next(slab_t *slab) {
    char *addr = reinterpret_cast<char *> (slab) + sizeof(slab_t);
    addr += slab->partial_pages_bucket_num * sizeof(uint32_t);
    return reinterpret_cast<slab_t *> (addr);
}

static hash_t *
hash_info(char *base) {
    return reinterpret_cast<hash_t *> (base + mem_pool::MEM_HASH_METADATA_START);
}

static mdb_item *
id2item(char *pool, int page_size, uint64_t id, slab_t **slabs) {
    char *addr = pool;
    addr += PAGE_ID(id) * page_size;
    addr += sizeof(mem_cache::page_info);
    addr += PAGE_OFFSET(id) * slabs[SLAB_ID(id)]->slab_size;
    return reinterpret_cast<mdb_item *>(addr);
}

static int
items_of_bucket(char *pool, uint64_t *ht, int bucket, slab_t **slabs) {
    uint64_t head = ht[bucket];
    int n = 0;
    while (head != 0) {
        ++n;
        mdb_item *item = id2item(pool, mdb_param::page_size, head, slabs);
        head = item->h_next;
    }
    return n;
}

char *open_shm(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "open %s: %m\n", path);
        return NULL;
    }
    size_t size = lseek(fd, 0, SEEK_END);
    char *addr = reinterpret_cast<char *> (mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (addr == MAP_FAILED) {
        fprintf(stderr, "mmap %s: %m\n", path);
        close(fd);
        return NULL;
    }
    close(fd);
    return addr;
}

bool
parse_params(int argc, char **argv, param_t *param) {
    int opt;
    const char *optstr = "havf:o:i";
    struct option long_opts[] = {
            {"help",     0, NULL, 'h'},
            {"verbose",  0, NULL, 'v'},
            {"shm_file", 1, NULL, 'f'},
            {"output",   1, NULL, 'o'},
            {"area",     0, NULL, 'a'},
            {NULL,       0, NULL, 0}
    };

    while ((opt = getopt_long(argc, argv, optstr, long_opts, NULL)) != -1) {
        switch (opt) {
            case 'f':
                param->shm_file = optarg;
                break;
            case 'o':
                param->out_file = optarg;
                break;
            case 'a':
                param->is_area_stat = true;
                break;
            case 'i':
                param->interactive = true;
                break;
            case 'v':
                param->verbose = true;
                break;
            case '?':
            case 'h':
            default :
                help();
                exit(1);
                break;
        }
    }
    if (param->shm_file == NULL) {
        fprintf(stderr, "babe, babe, give me the shm_file\n");
        help();
        return false;
    }
    return true;
}

void
help() {
    fprintf(stderr,
            "mdb_reader [options] <-f shm_file>\n"
                    "-h\n"
                    "--help\n"
                    "\tprint this message.\n"
                    "-v\n"
                    "--verbose\n"
                    "\tverbose mode.\n"
                    "-f\n"
                    "--shm_file\n"
                    "\tshm file that you wanna roll.\n"
                    "-o\n"
                    "--output\n"
                    "\tsave output to file.\n"
                    "-i\n"
                    "--interactive\n"
                    "\tinteractive mode.\n"
                    "-a\n"
                    "--area\n"
                    "\tfile is the area stat file.\n");
}
