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

#include "mem_cache.hpp"
#include "tblog.h"
#include "mdb_instance.hpp"
#include <string.h>
#include <vector>

//#define DEBUG_MEM_MERGE 1

namespace tair {

using namespace std;
data_dumpper mem_cache::item_dump;

bool mem_cache::initialize(int max_slab_id, int base_size, float factor) {
    assert(this_mem_pool != 0 && max_slab_id > 0 && base_size > 0);
    cache_info =
            reinterpret_cast <
                    mdb_cache_info * >(this_mem_pool->get_pool_addr() +
                                       mem_pool::MEM_POOL_METADATA_LEN);
    char *area_timestamp_start_addr = this_mem_pool->get_pool_addr() + mem_pool::MDB_AREA_TIME_STAMP_START;
    area_timestamp = reinterpret_cast<uint32_t *> (area_timestamp_start_addr);
    bucket_timestamp = reinterpret_cast<uint32_t *> (this_mem_pool->get_pool_addr() +
                                                     mem_pool::MDB_BUCKET_TIME_STAMP_START);

    if (cache_info->inited != 1) {
        cache_info->max_slab_id = max_slab_id;
        cache_info->base_size = base_size;
        cache_info->factor = factor;
        //init the timestamp
        memset(area_timestamp, 0, sizeof(uint32_t) * TAIR_MAX_AREA_COUNT);
        memset(bucket_timestamp, 0, sizeof(uint32_t) * TAIR_MAX_BUCKET_NUMBER);

        if (!slab_initialize()) {
            log_error("initialize slabs failed");
            return false;
        }
    } else {
        get_slab_info();                /* read from shared memory */
    }
    cache_info->inited = 1;
    return true;
}

#if 0
mdb_item *mem_cache::alloc_item(int size)
{
  SlabMng *slabmng = get_slabmng(size);
  if(slabmng == 0)
    return 0;
  return slabmng->alloc_item();
}
#endif

mdb_item *mem_cache::alloc_item(int size, int &type) {
    slab_manager *slabmng = get_slabmng(size);
    if (slabmng == 0)
        return 0;
    log_debug("size:%d,slabmng.slab_size : %d", size, slabmng->slab_size);
    return slabmng->alloc_item(type);
}

void mem_cache::update_item(mdb_item *item) {
    slab_managers[SLAB_ID(item->item_id)]->update_item(item, ITEM_AREA(item));
}

void mem_cache::free_item(mdb_item *item) {
    if (item == 0)
        return;
    slab_manager *slabmng = slab_managers.at(SLAB_ID(item->item_id));        //TODO catch exception
    assert(slabmng != 0);
    slabmng->free_item(item);
}

int mem_cache::free_page(int slab_id) {
    assert(slab_id >= 0 && slab_id < get_slabs_count());

    slab_manager *slab_mng = slab_managers.at(slab_id);
    assert(slab_mng != 0);

    if ((slab_mng->free_pages_no + slab_mng->full_pages_no
         + slab_mng->partial_pages_no) <= 1) {
        return -1;                //just have 1 page,we can't free the only page of this slab
    }
    page_info *info = 0;

    if (slab_mng->free_pages_no > 0) {
        goto FREE_PAGE;
    } else if (slab_mng->partial_pages_no > 0) {
        info = slab_mng->PAGE_INFO(this_mem_pool->index_to_page(
                slab_mng->get_the_most_free_items_of_partial_page_id()));
    } else if (slab_mng->full_pages_no > 0) {
        info =
                slab_mng->PAGE_INFO(this_mem_pool->index_to_page(slab_mng->full_pages));
    }

    assert(info != 0);
    clear_page(slab_mng, (char *) info); //clear_page will send the page into the mem_pool
    return 0;

    FREE_PAGE:
    assert(slab_mng->free_pages_no > 0);

    info =
            slab_mng->PAGE_INFO(this_mem_pool->index_to_page(slab_mng->free_pages));
    assert(info != 0);
    assert(info->free_nr == slab_mng->per_slab
           && info->free_head != 0 && info->free_nr <= slab_mng->per_slab);
    slab_mng->unlink_page(info, slab_mng->free_pages);
    --slab_mng->free_pages_no;
    this_mem_pool->free_page(info->id);

    return 0;
}

int mem_cache::get_slabs_count() {
    return cache_info->max_slab_id + 1;
}

bool mem_cache::is_quota_exceed(int area) {
    //return false; //for debug
    return instance->is_quota_exceed(area);
}

void mem_cache::calc_slab_balance_info(std::map<int, int> &adjust_info) {
    double crrnt_no = 0.0;
    double should_have_no = 0.0;

    double evict_ratio;

    int need_alloc_page = 0;

    adjust_info.clear();

    for (vector<slab_manager *>::iterator it = slab_managers.begin();
         it != slab_managers.end(); ++it) {
        if ((*it)->item_total_count == 0) {        //
            continue;
        }
        crrnt_no += (*it)->item_total_count;
        should_have_no += ((*it)->item_total_count + (*it)->evict_total_count);
    }

    if (should_have_no == 0 || crrnt_no == 0) {
        //don't worry about "float compare"
        return;                        //just in case
    }

    evict_ratio = should_have_no / crrnt_no;

    log_warn("overall: items + evicts = %.0f, items = %.0f, evict_ratio = %f",
             should_have_no, crrnt_no, evict_ratio);

    for (vector<slab_manager *>::iterator it = slab_managers.begin();
         it != slab_managers.end(); ++it) {
        if ((*it)->item_total_count == 0) {
            continue;
        }
        should_have_no = ((*it)->item_total_count + (*it)->evict_total_count);
        int final_count = static_cast<int>(should_have_no / evict_ratio);
        int need_count = final_count - (*it)->item_total_count;

        int need_page =
                (::abs(need_count) + (*it)->per_slab - 1) / (*it)->per_slab;
        need_page = need_count > 0 ? need_page : -need_page;
        /*
         * `need_page` less than zero indicates that evicting in this slab
         * happens less frequently, thus you may squeeze this slab when memory
         * is insufficient.
         */

        /*
         * also note that `evict_total_count` is increased only when `alloc_page` failed.
         */

        log_warn("slab %d: items = %lu, evicts = %lu, need_count = %d, need_page = %d",
                 (*it)->slab_id, (*it)->item_total_count, (*it)->evict_total_count, need_count, need_page);

        adjust_info.insert(make_pair<int, int>((*it)->slab_id, need_page));

        need_alloc_page += need_count > 0 ? need_page : 0;
    }

    if (need_alloc_page <= this_mem_pool->get_free_pages_num()) {
        //~ weird, but page pool has enough free pages, need not squeeze
        for (map<int, int>::iterator it = adjust_info.begin();
             it != adjust_info.end(); ++it) {
            if (it->second < 0) {
                it->second = 0;
            }
        }

    }
    for (map<int, int>::iterator it = adjust_info.begin();
         it != adjust_info.end(); ++it) {
        if (it->second != 0) {
            log_warn("slab %d needs %d page(s)", it->first, it->second);
        }
    }

    return;

}

void mem_cache::balance_slab_done() {
    for (vector<slab_manager *>::iterator it = slab_managers.begin();
         it != slab_managers.end(); ++it) {
        (*it)->evict_total_count = 0;
    }
    return;
}

bool mem_cache::keep_area_quota(int area, uint64_t &exceeded_bytes) {
    assert(area >= 0 && area < TAIR_MAX_AREA_COUNT);

    bool no_space_to_release = true;

    for (vector<slab_manager *>::iterator it = slab_managers.begin();
         it != slab_managers.end(); ++it) {
        if ((*it)->this_item_list[area].item_head == 0
            || (*it)->this_item_list[area].item_tail == 0) {
            // this slab have no mdb_item of this area
            continue;
        }

        mdb_item *item = (*it)->id_to_item((*it)->this_item_list[area].item_tail);
        if (exceeded_bytes >= (uint64_t) (item->key_len + item->data_len)) {
            exceeded_bytes -= (item->key_len + item->data_len);
        } else {
            exceeded_bytes = 0;
        }
        instance->__remove(item);
        no_space_to_release = false;

        if (exceeded_bytes <= 0) {
            break;
        }
    }

    if (no_space_to_release) {
        return false;
    }
    return true;
}

uint64_t mem_cache::get_item_head(int slab_id, int area) {
    assert(slab_id < get_slabs_count());
    assert(area < TAIR_MAX_AREA_COUNT);
    slab_manager *slab_mng = slab_managers.at(slab_id);
    assert(slab_mng != 0);
    return slab_mng->get_item_head(area);
}

void mem_cache::display_statics() {
    log_warn("total slab : %lu", slab_managers.size());

    for (vector<slab_manager *>::iterator it = slab_managers.begin();
         it != slab_managers.end(); ++it) {
        (*it)->display_statics();
    }
}


mem_cache::slab_manager *mem_cache::get_slabmng(int size) {
    int target_slab_id = find_target_slab(size);
    if (LIKELY(target_slab_id < (int) slab_managers.size())) {
        return slab_managers[target_slab_id];
    }
    return NULL;
}

bool mem_cache::slab_initialize() {
    if (cache_info->base_size < (int) (sizeof(mdb_item) + 16)) {
        cache_info->base_size = ALIGN(sizeof(mdb_item) + 16);        /* at least 16 bytes */
    }
    char *slab_start_addr =
            this_mem_pool->get_pool_addr() + mem_pool::MEM_POOL_METADATA_LEN + sizeof(mdb_cache_info);

    log_debug("pool:%p,slab_start:%p", this_mem_pool->get_pool_addr(),
              slab_start_addr);

    int page_size = this_mem_pool->get_page_size();
    assert(cache_info->base_size < page_size);

    int start = cache_info->base_size;
    int end = page_size - sizeof(page_info);

    int total_size = 0;
    char *next_slab_addr = slab_start_addr;
    int i = 0;
    for (; i < cache_info->max_slab_id && start <= end; ++i) {

        if (start > static_cast<int>(end / 2)) {        //page_size slab
            start = end;
        }
        //get slabmng
        slab_manager *slabmng =
                reinterpret_cast<slab_manager *>(next_slab_addr);

        //initialize
        slabmng->this_mem_pool = this_mem_pool;
        slabmng->cache = this;
        slabmng->slab_id = i;
        slabmng->slab_size = start;
        slabmng->per_slab = (page_size - sizeof(page_info)) / start;
        slabmng->partial_pages_bucket_num = (slabmng->per_slab + slab_manager::PARTIAL_PAGE_BUCKET - 1)
                                            / slab_manager::PARTIAL_PAGE_BUCKET;
        slabmng->page_size = page_size;
        slabmng->evict_index = 0;
        slabmng->evict_total_count = 0;
        slabmng->item_total_count = 0;
        slabmng->first_partial_page_index = 0;
        slabmng->partial_bucket_count = 0;
        slabmng->partial_pages =
                reinterpret_cast< uint32_t *>(next_slab_addr + sizeof(slab_manager));

        log_debug("slab: id:%d,size:%d,per_slab:%d", i, start,
                  slabmng->per_slab);
        //prealloc
        if (slabmng->pre_alloc(1) < 1) {
            log_error("init slab[%d] failed: need more memory or less instances", slabmng->slab_id);
            return false;
        }
        slab_managers.push_back(slabmng);
        slab_size.push_back(start);

        //next slab
        next_slab_addr +=
                slabmng->partial_pages_bucket_no() * sizeof(uint32_t) +
                sizeof(slab_manager);
        start = ALIGN((int) (start * cache_info->factor));
        total_size += sizeof(slab_manager);
        total_size += slabmng->partial_pages_bucket_no() * sizeof(uint32_t);

    }
    assert(total_size < mem_cache::MEMCACHE_META_LEN);
    log_debug("total_size:%d,meta:%d", total_size,
              mem_cache::MEMCACHE_META_LEN);
    cache_info->max_slab_id = i - 1;

    return true;
}

bool mem_cache::get_slab_info() {
    char *next_slab_addr =
            this_mem_pool->get_pool_addr() + mem_pool::MEM_POOL_METADATA_LEN + sizeof(mdb_cache_info);

    for (int i = 0; i <= cache_info->max_slab_id; ++i) {
        slab_manager *slabmng = reinterpret_cast<slab_manager *>(next_slab_addr);
        slabmng->this_mem_pool = this_mem_pool;
        slabmng->cache = this;
        slabmng->partial_pages = reinterpret_cast< uint32_t *>(next_slab_addr + sizeof(slab_manager));
        slab_managers.push_back(slabmng);
        slab_size.push_back(slabmng->slab_size);
        next_slab_addr += slabmng->partial_pages_bucket_no() * sizeof(uint32_t) +
                          sizeof(slab_manager);
    }
    return true;
}

bool mem_cache::is_need_mem_merge(slab_manager *manager) {
    int partial_page_counts = manager->item_total_count - manager->full_pages_no * manager->per_slab;
    int min_part_pages_no = (int) ((partial_page_counts + manager->per_slab - 1) / manager->per_slab);
    return (partial_page_counts > 1 && manager->partial_pages_no > min_part_pages_no);
}

void mem_cache::copy_item(mdb_item *to_item, mdb_item *from_item) {
    // item_id partial
    to_item->prefix_size = from_item->prefix_size;
    to_item->low_hash = from_item->low_hash;
    to_item->flags = from_item->flags;

    to_item->h_next = from_item->h_next;
    to_item->prev = from_item->prev;
    to_item->next = from_item->next;

    to_item->exptime = from_item->exptime;
    to_item->key_len = from_item->key_len;
    to_item->data_len = from_item->data_len;
    to_item->update_time = from_item->update_time;
    to_item->version = from_item->version;

    // copy data
    memcpy(to_item->data, from_item->data, from_item->key_len + from_item->data_len);
}

bool mem_cache::do_slab_memory_merge(volatile bool &stopped, slab_manager *manager) {
    int partial_page_counts = manager->item_total_count - manager->full_pages_no * manager->per_slab;
    int min_part_pages_no = (int) ((partial_page_counts + manager->per_slab - 1) / manager->per_slab);
    int need_free_count = manager->partial_pages_no - min_part_pages_no;
    int current_free_count = 0;

    // get partial pages vector
    std::vector<int> partial_pages;
    int buck_idx = manager->first_partial_page_index;
    for (; buck_idx < manager->partial_pages_bucket_num; ++buck_idx) {
        uint32_t page_id = manager->partial_pages[buck_idx];
        while (page_id != 0) {
            partial_pages.push_back(page_id);
            page_info *info = (page_info *) (this_mem_pool->index_to_page(page_id));
            log_info("found partial pages, id: %d, bucket idx:%d, free_nr: %d", page_id, buck_idx,
                      info->free_nr);
            page_id = info->next;
        }
    }
    log_info("partial pages is %d, we found %d", manager->partial_pages_no, (int) partial_pages.size());

    if (partial_pages.empty()) {
        return false;
    }

    int first_partial_page_id = partial_pages[0];
    partial_pages.erase(partial_pages.begin());

    int last_partial_page_id = partial_pages.back();
    partial_pages.pop_back();

    // find used item offset
    uint32_t used_item_page_off = 0;
    int move_count = 0;
    while (!stopped && current_free_count < need_free_count && ++move_count < mdb_param::mem_merge_move_count) {
        // 1. get old item from last page
        mdb_item *from_item = manager->get_used_item_by_page_id(last_partial_page_id, used_item_page_off);
        // update item offset for next find
        if (from_item == NULL || from_item->page_id != last_partial_page_id || from_item->slab_id != manager->slab_id) {
            page_info *error_page = PAGE_INFO(this_mem_pool->index_to_page(last_partial_page_id));
            log_error("page_off_idx(%u) found error, item addr: %p", used_item_page_off, from_item);
            log_error("error page id: %u, free_item: %d, used_item: %d",
                      error_page->id, error_page->free_nr, manager->per_slab - error_page->free_nr);
            log_warn("cannot found any used item in this page, do nothing and return.");
            return false;
        }
        page_info *check_page = PAGE_INFO(this_mem_pool->index_to_page(last_partial_page_id));
        log_info("page_off_idx(%u) found, item addr: %p", used_item_page_off, from_item);
        log_info("check page id: %u, free_item: %d, used_item: %d",
                  check_page->id, check_page->free_nr, manager->per_slab - check_page->free_nr);

        used_item_page_off = from_item->page_off + 1;

        // 2. alloc new mdb_item from first partial page
        bool is_link_full = false;
        mdb_item *to_item = manager->alloc_item_from_page(first_partial_page_id, is_link_full);
        if (is_link_full) {
            ++current_free_count;
            if (!partial_pages.empty()) {
                first_partial_page_id = partial_pages[0];
                partial_pages.erase(partial_pages.begin());
            } else {
                log_info("partial_pages is empty, last copy.");
            }
        }

        // 3. copy data and other control info
        copy_item(to_item, from_item);

        // 4. change `cache_hashmap` pointer of old item
        if (!instance->get_cache_hashmap()->change_item_pointer(to_item, from_item)) {
            log_error("Hashmap pointer Error, this slab manager have not partial page!");
            return false;
        }

        // 5. change `this_item_list` pointer of new item
        if (!manager->change_item_list_pointer(to_item, from_item)) {
            log_error("this_item_list pointer Error, this slab manager have not partial page!");
            return false;
        }

        // 6. free old item in page(maybe free page)
        if (manager->free_item_except_area_list(from_item) == 1) {
            ++current_free_count;
            used_item_page_off = 0;
            // update last partial page id
            last_partial_page_id = partial_pages.back();
            partial_pages.pop_back();
        }
    }

    return true;
}

void mem_cache::slab_memory_merge(volatile bool &stopped, pthread_mutex_t *mdb_lock) {
    double all_merge_time = 0;
    double max_merge_time = 0.0;
    double min_merge_time = 9999.0;

    pthread_mutex_lock(mdb_lock);

    for (size_t slab_idx = 0; slab_idx < slab_managers.size(); ++slab_idx) {
        slab_manager *manager = slab_managers[slab_idx];
        if (!is_need_mem_merge(manager)) {
            continue;
        }

        int partial_page_counts = manager->item_total_count - manager->full_pages_no * manager->per_slab;
        int min_part_pages_no = (int) ((partial_page_counts + manager->per_slab - 1) / manager->per_slab);

        log_info("mdb instance[%d] running memory merge now ...", instance->instance_id());

        log_info("slab_manager[%d] running memory merge, It's item is %lu, full pages: %d, one page has %d items",
                  manager->slab_id, manager->item_total_count, manager->full_pages_no, manager->per_slab);

        log_info("slab_manager[%d] running memory merge, It's partial_pages_no is %d, but needs min_pages_no is %d",
                  manager->slab_id, manager->partial_pages_no, min_part_pages_no);

        double start_time = get_timespec();

        if (!do_slab_memory_merge(stopped, manager)) {
            log_error("slab_manager[%d] disruption!!", manager->slab_id);
        }

        double end_time = get_timespec();
        double used_time = end_time - start_time;

        all_merge_time += used_time;
        if (max_merge_time < used_time) {
            max_merge_time = used_time;
        }
        if (min_merge_time > used_time) {
            min_merge_time = used_time;
        }
        log_warn("slab_manager[%d] merge used %lf ms", manager->slab_id, used_time * 1000);

        if (stopped) {
            break;
        }

#ifdef DEBUG_MEM_MERGE
        log_error("---------------- manager count: %lu ---------------", manager->item_total_count);
        int area_item_count = 0;
        for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
          uint64_t item_id = (&(manager->this_item_list[i]))->item_head;
          while (item_id) {
            mdb_item *item = id_to_item(item_id);
            item_id = item->next;
            ++area_item_count;
          }
        }
        log_error("---------------- all area count: %d ---------------", area_item_count);

        log_error("---------------- hashmap record count: %d ---------------", instance->get_cache_hashmap()->get_item_count());
        int hash_item_count = 0;
        uint64_t *hashmap = instance->get_cache_hashmap()->get_hashmap();
        int bucket_size = instance->get_cache_hashmap()->get_bucket_size();
        for (int i = 0; i < bucket_size;++i) {
          uint64_t item_id = hashmap[i];
          while (item_id) {
            ++hash_item_count;
            mdb_item *item = id_to_item(item_id);
            item_id = item->h_next;
          }
        }
        log_error("---------------- found hashmap count: %d ---------------", hash_item_count);
#endif
    }

    pthread_mutex_unlock(mdb_lock);

    if (min_merge_time > max_merge_time) {
        min_merge_time = max_merge_time;
    }
    if (max_merge_time != 0) {
        log_warn("instance[%d] finished, max: %lf ms  min: %lf ms",
                  instance->instance_id(), max_merge_time * 1000, min_merge_time * 1000);
    }
}


#ifdef TAIR_DEBUG
map<int, int>mem_cache::get_slab_size()
{
  map <int, int>slabs;
  for(vector<slab_manager *>::iterator it = slab_managers.begin();
      it != slab_managers.end(); ++it) {
    int pages = (*it)->full_pages_no + (*it)->partial_pages_no;
    int slabId = (*it)->slab_id;
    pair<int, int>p = make_pair(slabId, pages);
    pair<map<int, int >::iterator, bool > re = slabs.insert(p);
    assert(re.second);
  }

  return slabs;
}

vector<int>mem_cache::get_area_size()
{
  vector <int>areas;
  int i = 0;
  for(vector<slab_manager *>::iterator it = slab_managers.begin();
      it != slab_managers.end(); ++it, ++i) {
    int num = (*it)->item_count[i];
    int size = (*it)->slab_size;
    areas.push_back(num * size);
  }
  return areas;
}
#endif

/*-----------------------------------------------------------------------------
 *  mem_cache::SlabMng
 *-----------------------------------------------------------------------------*/

mdb_item *mem_cache::slab_manager::alloc_new_item(int area) {
    char *page = 0;
    mdb_item *item = 0;
    uint64_t item_id = 0;
    page_info *info = 0;

    log_debug("alloc_new_item,area : %d", area);
    if (partial_pages_no > 0) {
        PROFILER_BEGIN("partial page");
        uint32_t partial_page_id = get_partial_page_id();
        log_debug("alloc from partial page:%u", partial_page_id);
        assert(partial_page_id > 0);
        info = PAGE_INFO(this_mem_pool->index_to_page(partial_page_id));
        PROFILER_END();
        assert(info != 0);
        assert(info->free_nr > 0);
        assert(info->free_head != 0);
        assert(info->free_nr <= per_slab);

        item_id = info->free_head;
        unlink_partial_page(info);
    } else if (free_pages_no > 0) {
        log_debug("alloc from free page : %u", free_pages);
        info = PAGE_INFO(this_mem_pool->index_to_page(free_pages));
        assert(info != 0);
        assert(info->free_nr == per_slab);
        assert(info->free_head != 0);
        item_id = info->free_head;
        unlink_page(info, free_pages);
        --free_pages_no;
        ++partial_pages_no;

    } else {
        //allocate a new page
        log_debug("allocate a new page");
        int index;
        PROFILER_BEGIN("alloc page");
        page = this_mem_pool->alloc_page(index);
        PROFILER_END();
        if (page == 0) {
            log_debug("allocate a new page falied");
            return 0;
        }
        PROFILER_BEGIN("init page");
        init_page(page, index);
        PROFILER_END();
        info = PAGE_INFO(page);
        assert(info != 0);
        item_id = info->free_head;
        ++partial_pages_no;
    }

    //TODO(maoqi),CLEAR_FLAGS(item_id);

    //page : m_free_pages -> m_partial_pages -> m_full_pages
    //every page will obey this rule,even though the page that has only one item.

    --(info->free_nr);
    if (info->free_nr == 0) {
        link_page(info, full_pages);
        ++full_pages_no;
        --partial_pages_no;
        info->free_head = 0;
    } else {
        log_debug("link into partial_pages[%d]",
                  info->free_nr / PARTIAL_PAGE_BUCKET);
        link_partial_page(info);
    }

    assert(item_id > 0);

    ++item_total_count;
    item = id_to_item(item_id);
    assert(item != 0);

    if (info->free_nr != 0) {
        info->free_head = item->h_next;
    }

    log_debug("from page id:%u,now free:%d,free-head:%lu,id will be use:%lu", info->id, info->free_nr,
              info->free_head, item_id);

    link_item(item, area);

    if (item->item_id != item_id) {
        log_error("why? overwrite? item->item_id [%lu],item_id[%lu]", item->item_id, item_id);
        assert(0);
    }

    if (info->free_nr != 0 && info->free_head == 0) {
        log_error("the free nr of page is greater than 0,buf the free_head is zero");
        log_error("item:%p,page:%p", item, info);
        assert(0);
    } else if (info->free_nr == 0) {
        assert(info->free_head == 0);
    } else {
        if (PAGE_ID((info->free_head)) != info->id) {
            log_error("the page id of free_head [%ld] != info->id [%u]", PAGE_ID(info->free_head), info->id);
        }
    }
    return item;
}

/**
 * @brief alloc an mdb_item
 *
 * @param type @in area @out alloc type
 *
 * @return  the address of mdb_item on success, 0 on failed
 */
mdb_item *mem_cache::slab_manager::alloc_item(int &type) {
    //TODO check the quota of this area
    //mdb_item *it = 0;
    //if quota is enough,then
    //        it = alloc_item();
    //        if it == 0,then
    //                it = evict_self();
    //                if it == 0,then
    //                        it = evict_any();
    //else
    //        it = evict_self();
    //        if it == 0,then
    //                it = alloc_item();
    //                if it == 0,then
    //                        it = evict_any();
    // real code:
    //
    // if quota is enough:
    //        goto ALLOC_NEW;
    // else
    //        goto EVICT_SELF;
    //
    // ALLOC_NEW:
    //        it = alloc_item();
    //        if (it == 0 && have_quota)
    //                goto EVICT_SELF;
    //        else if (it == 0)
    //                goto EVICT_ANY;
    //        return it;
    // EVICT_SELF:
    //        it = evict_self();
    //        if (it == 0 && have_quota)
    //                goto EVICT_ANY;
    //        else if (it == 0)
    //                goto ALLOC_NEW;
    //        return it;
    // EVICT_ANY:
    //        it = evict_any();
    //        return it;
    //
    mdb_item *it = 0;
    int area = type;
    log_debug("alloc_item,area:%d", area);
    assert(area < TAIR_MAX_AREA_COUNT);
    bool exceed = cache->is_quota_exceed(area);

    if (!exceed) {
        goto ALLOC_NEW;
    } else {
        log_debug("quota is exceed,evict_self");
        goto EVICT_SELF;
    }
    ALLOC_NEW:
    PROFILER_BEGIN("alloc new");
    it = alloc_new_item(area);
    PROFILER_END();
    if (it == 0 && !exceed) {
        goto EVICT_SELF;
    } else if (it == 0) {
        goto EVICT_ANY;
    }
    type = ALLOC_NEW;

    /*Stat info */
    ++item_count[area];
    return it;
    EVICT_SELF:
    PROFILER_BEGIN("evict self");
    it = evict_self(type);
    PROFILER_END();
    if (it == 0 && !exceed) {
        goto EVICT_ANY;
    } else if (it == 0) {
        goto ALLOC_NEW;
    }

    if (type == ALLOC_EVICT_SELF && !exceed) {
        ++evict_total_count;
        ++evict_count[area];
    }
    return it;
    EVICT_ANY:
    PROFILER_BEGIN("evict any");
    it = evict_any(type);
    PROFILER_END();
    assert(it != 0);

    if (type == ALLOC_EVICT_ANY) {
        ++evict_total_count;
        ++evict_count[ITEM_AREA(it)];
    }
    --item_count[ITEM_AREA(it)];
    ++item_count[area];
    return it;
}

void mem_cache::slab_manager::update_item(mdb_item *item, int area) {
    unlink_item(item);
    link_item(item, area);
}

bool mem_cache::slab_manager::change_item_list_pointer(mdb_item *to_item, mdb_item *from_item) {
    if (!to_item || !from_item || to_item->item_id == 0 || from_item->item_id == 0) {
        log_error("to_item or form_item is NULL. Why?");
        return false;
    }

    if (ITEM_AREA(from_item) != ITEM_AREA(to_item)) {
        log_error("to_item & form_item are not same area. Why?");
        return false;
    }

    item_list *head = &this_item_list[ITEM_AREA(from_item)];
    if (itemid_equal(head->item_head, from_item->item_id)) {
        head->item_head = to_item->item_id;
    }
    if (itemid_equal(head->item_tail, from_item->item_id)) {
        head->item_tail = to_item->item_id;
    }

    if (from_item->prev) {
        mdb_item *item_prev = id_to_item(from_item->prev);
        if (!itemid_equal(item_prev->next, from_item->item_id)) {
            log_error("area item list error! return false!");
            return false;
        }
        item_prev->next = to_item->item_id;
    }
    if (from_item->next) {
        mdb_item *item_next = id_to_item(from_item->next);
        if (!itemid_equal(item_next->prev, from_item->item_id)) {
            log_error("area item list error! return false!");
            return false;
        }
        item_next->prev = to_item->item_id;
    }

    return true;
}

void mem_cache::slab_manager::free_item(mdb_item *item) {
    assert(item != 0 && item->item_id != 0);
    --item_total_count;
    unlink_item(item);
    page_info *info =
            PAGE_INFO(this_mem_pool->index_to_page(item->page_id));
    if (info->free_nr == 0) {
        unlink_page(info, full_pages);
        --full_pages_no;
        ++partial_pages_no;
    } else {
        assert(info->free_head != 0);
        unlink_partial_page(info);
    }

    item->prev = 0;
    item->next = 0;
    item->prefix_size = 0;
    item->low_hash = 0;

    log_debug("id:%lu before free:page id:%u,%p,info->free_nr:%d,info->free_head:%lu,SLAB_ID(id):%lu,",
              item->item_id, info->id, info, info->free_nr, info->free_head, SLAB_ID(item->item_id));

    item->h_next = info->free_head;
    info->free_head = item->item_id;


    if (++info->free_nr == per_slab) {
        log_debug("this page is all free:%d,free_nr:%d", info->id, info->free_nr);
        --partial_pages_no;

        if (free_pages_no + full_pages_no + partial_pages_no <= 1) {
            log_debug("only one page,don't free");
            link_page(info, free_pages);
            ++free_pages_no;
        } else {
            log_debug("send this page to pool");
            this_mem_pool->free_page(info->id);
        }
    } else {
        link_partial_page(info);
    }

    log_debug("after free:page id:%u,%p,info->free_nr:%d,info->free_head:%lu,SLAB_ID(id):%lu,info->free_head->next : %lu",
              info->id, info, info->free_nr, info->free_head, SLAB_ID(item->item_id), item->h_next);
}

mdb_item *mem_cache::slab_manager::get_used_item_by_page_id(uint32_t page_id, uint32_t page_off_idx) {
    if (page_off_idx >= (uint32_t) per_slab) {
        return NULL;
    }

    page_info *info = PAGE_INFO(this_mem_pool->index_to_page(page_id));

    if (info == NULL || info->free_nr == 0 || info->free_head == 0 || info->free_nr == per_slab) {
        log_error("Page ID Error, this slab manager have not partial page, check Slab Merge Algorithm!");
        return NULL;
    }

    mdb_item *item_iter = NULL;
    for (int i = page_off_idx; i < per_slab; ++i) {
        // item addr = page_addr + sizeof(page_info) + slab_size * page_off
        item_iter = reinterpret_cast<mdb_item *>((char *) info + sizeof(page_info) + slab_size * i);

        // when a item in head or tail of `this_item_list`, It's prev or next maybe is 0
        item_list *head = &this_item_list[ITEM_AREA(item_iter)];
        if (item_iter->prev != 0 || item_iter->next != 0 ||
            itemid_equal(head->item_head, item_iter->item_id) ||
            itemid_equal(head->item_tail, item_iter->item_id)) {
            return item_iter;
        }
    }

    return NULL;
}

mdb_item *mem_cache::slab_manager::alloc_item_from_page(int page_id, bool &is_link_full) {
    page_info *info = NULL;
    mdb_item *new_item = NULL;
    uint64_t item_id = 0;

    info = PAGE_INFO(this_mem_pool->index_to_page(page_id));
    log_info("alloc new item in page(%d), item count %d -> %d", page_id, info->free_nr, info->free_nr - 1);

    if (info == NULL || info->free_nr == 0 || info->free_head == 0) {
        log_error("Page ID Error, this slab manager have not partial page, check Slab Merge Algorithm!");
        return NULL;
    }
    if (info->free_nr == 0) {
        log_error("Partial Page ID Error, rm shm files and check Slab Merge Algorithm!");
        return NULL;
    }

    unlink_partial_page(info);

    item_id = info->free_head;
    new_item = id_to_item(item_id);
    if (new_item == NULL || new_item->item_id != item_id) {
        log_error("Item ID Error, rm shm files and check Slab Merge Algorithm!");
        return NULL;
    }

    --(info->free_nr);

    if (info->free_nr == 0) {
        log_info("page(%d) is full, we move it.", page_id);
        is_link_full = true;
        info->free_head = 0;
        // this function update the `first_partial_page_index`
        link_page(info, full_pages);
        --partial_pages_no;
        ++full_pages_no;
    } else {
        is_link_full = false;
        info->free_head = new_item->h_next;
        link_partial_page(info);
    }

    return new_item;
}

int mem_cache::slab_manager::free_item_except_area_list(mdb_item *item) {
    page_info *info = PAGE_INFO(this_mem_pool->index_to_page(item->page_id));

    unlink_partial_page(info);

    item->prev = 0;
    item->next = 0;
    item->prefix_size = 0;
    item->low_hash = 0;

    item->h_next = info->free_head;
    info->free_head = item->item_id;

    log_info("page(%u), free_item: %d -> %d", info->id, info->free_nr, info->free_nr + 1);
    ++(info->free_nr);

    if (info->free_nr == per_slab) {
        log_info("happy to free page(%d)", info->id);
        --partial_pages_no;
        this_mem_pool->free_page(info->id);
        return 1;
    } else {
        link_partial_page(info);
    }

    return 0;
}

mdb_item *mem_cache::slab_manager::evict_self(int &type) {
    uint32_t crrnt_time = time(NULL);
    int times = 50;
    mdb_item *item = 0;
    item_list *head = &this_item_list[type];
    int area = type;
    log_debug("evict_self,area:%d", type);
    uint64_t pos = head->item_tail;
    log_debug("tail:%lu", pos);

    if (pos == 0) {                // this area have no item of this slab
        log_debug("this area have no item of this size,area:%d,size:%d",
                  type, slab_size);
        return 0;
    }
    bool found = false;
    while (times-- > 0 && pos != 0) {
        item = id_to_item(pos);
        assert(item != 0);
        if (item->exptime > 0 && item->exptime < crrnt_time) {
            type = ALLOC_EXPIRED;
            found = true;
            break;
        }
        pos = item->prev;
    }
    if (!found) {
        type = ALLOC_EVICT_SELF;
        pos = head->item_tail;
        item = id_to_item(pos);
        if (ITEM_AREA(item) != area) {
            log_error("item in [%d] list is not my item [%d]", area, ITEM_AREA(item));
            assert(0);
        }
        dump_item(item);
    }
    item->flags = 0;
    PROFILER_BEGIN("update item");
    update_item(item, ITEM_AREA(item));
    PROFILER_END();
    return item;
}

/*
 * called after alloc_item or evict_self
 */

mdb_item *mem_cache::slab_manager::evict_any(int &type) {
    assert(type < TAIR_MAX_AREA_COUNT);
    if (evict_index >= TAIR_MAX_AREA_COUNT) {
        evict_index = 0;
    }
    log_info("evict any,area:%d", type);
    uint32_t crrnt_time = time(NULL);
    mdb_item *item = 0;
    int times = 0;
    int area = type;
    type = ALLOC_EVICT_ANY;
    while (evict_index < TAIR_MAX_AREA_COUNT) {
        item_list *head = &this_item_list[evict_index];
        uint64_t pos = head->item_tail;

        if (pos != 0) {
            item = id_to_item(pos);
            if (item->exptime > 0 && item->exptime < crrnt_time) {
                type = ALLOC_EXPIRED;        //
            }
            break;
        }

        if (++evict_index >= TAIR_MAX_AREA_COUNT) {
            evict_index = 0;
        }

        /*
         * !TODO
         * when compiled with `-O2' & `-DNDEBUG', this `if' might be optimized out,
         * cause of the possible infinite loop
         */
        if (++times > TAIR_MAX_AREA_COUNT) {
            assert(false);                //shoudn't be here,just in case
        }
    }
    item->flags = 0;
    PROFILER_BEGIN("update item");
    update_item(item, area);
    PROFILER_END();
    return item;
}

void mem_cache::slab_manager::init_page(char *page, int index) {
    page_info *info = PAGE_INFO(page);
    info->id = index;
    info->free_nr = per_slab;
    info->free_head = ITEM_ID(0, index, slab_id);

    log_debug("new page:%p,slab_size:%d,free_nr:%d,m_id:%d,free_head:%lu", page, slab_size,
              per_slab, index, info->free_head);

    char *item_start = page + sizeof(page_info);
    mdb_item *item = reinterpret_cast<mdb_item *>(item_start);
    for (int i = 0; i < per_slab; ++i) {
        memset(item, 0, sizeof(*item));
        item->next = item->prev = 0;
        item->page_off = i;
        item->page_id = index;
        item->slab_id = slab_id;
        item->h_next = ITEM_ID(i + 1, index, slab_id);
        log_debug("in page [%d],the %dth item: %lu,h_next :%lu", info->id, i, item->item_id, item->h_next);
        item = reinterpret_cast<mdb_item *>((char *) item + slab_size);
    }
    item = reinterpret_cast<mdb_item *>((char *) item - slab_size);
    item->h_next = 0;
}

void mem_cache::clear_page(slab_manager *slab_mng, char *page) {
    page_info *info = reinterpret_cast<page_info *> (page);
    log_warn("clearing page %u, %d items would be freed", info->id, slab_mng->per_slab - info->free_nr);
    char *item_start = page + sizeof(page_info);
    mdb_item *item = 0;
    for (int i = 0; i < slab_mng->per_slab; ++i) {
        item = reinterpret_cast<mdb_item *>(item_start + i * slab_mng->slab_size);
        if (item->prev == 0 && item->next == 0) {
            continue;                //is free
        }
        instance->__remove(item);
    }
}

void mem_cache::slab_manager::link_item(mdb_item *item, int area) {
    assert(item != 0);
    assert(item->item_id != 0);
    assert(area < TAIR_MAX_AREA_COUNT);
    item_list *head = &this_item_list[area];
    log_debug("link_item [%p] [%lu] into [%d] [%p],list", item, item->item_id, area, head);

    item->next = head->item_head;
    item->prev = 0;
    if (item->next != 0) {
        mdb_item *old_head = id_to_item(item->next);
        old_head->prev = item->item_id;
    }
    head->item_head = item->item_id;

    if (head->item_tail == 0) {
        head->item_tail = item->item_id;
    }
}

void mem_cache::slab_manager::unlink_item(mdb_item *item) {
    assert(item != 0);
    assert(item->item_id != 0);

    mdb_item *prev = 0;
    mdb_item *next = 0;

    item_list *head = &this_item_list[ITEM_AREA(item)];

    log_debug("unlink item [%p] [%lu] from [%d] [%p],list", item, item->item_id, ITEM_AREA(item), head);

    if (itemid_equal(item->item_id, head->item_head)) {
        head->item_head = item->next;
    }

    if (itemid_equal(item->item_id, head->item_tail)) {
        head->item_tail = item->prev;
    }

    if (item->prev != 0) {
        prev = id_to_item(item->prev);
        prev->next = item->next;
    }

    if (item->next != 0) {
        next = id_to_item(item->next);
        next->prev = item->prev;
    }
}

void mem_cache::slab_manager::link_page(page_info *info,
                                        uint32_t &page_head) {
    assert(info != 0);
    info->prev = 0;
    info->next = page_head;
    if (info->next != 0) {
        page_info *next_pageinfo =
                PAGE_INFO(this_mem_pool->index_to_page(info->next));
        next_pageinfo->prev = info->id;
    }
    page_head = info->id;
}

void mem_cache::slab_manager::unlink_page(page_info *info,
                                          uint32_t &page_head) {
    assert(info != 0);
    if (info->id == page_head) {
        page_head = info->next;
    }
    if (info->next) {
        page_info *next_pageinfo =
                PAGE_INFO(this_mem_pool->index_to_page(info->next));
        next_pageinfo->prev = info->prev;
    }
    if (info->prev) {
        page_info *prev_pageinfo =
                PAGE_INFO(this_mem_pool->index_to_page(info->prev));
        prev_pageinfo->next = info->next;
    }
    info->prev = info->next = 0;
}

int mem_cache::slab_manager::pre_alloc(int pages) {
    assert(pages > 0);
    int index = 0;
    char *page = 0;
    page_info *info = 0;

    int allocated = 0;
    for (int i = 0; i < pages; ++i) {
        page = this_mem_pool->alloc_page(index);
        if (page == 0) {
            log_debug("allocate page falied");
            return allocated;
        }
        ++free_pages_no;

        init_page(page, index);
        info = PAGE_INFO(page);

        link_page(info, free_pages);

        ++allocated;
    }
    return allocated;
}

void mem_cache::slab_manager::display_statics() {
    log_debug("slab_id:%u,slab_size:%d,per_slab:%d\n", slab_id,
              slab_size, per_slab);
    log_debug("item_total_count:%lu,full_pages_no:%d,partial_pages_no:%d,free_pages_no:%d\n",
              item_total_count, full_pages_no, partial_pages_no, free_pages_no);
}

void mem_cache::slab_manager::dump_item(mdb_item *item) {
    if (item == 0) {
        return;
    }
    log_debug("\033[;31mevict data,start dump ...\033[0;m");
    data_dumpper::item t;
    t.area = ITEM_AREA(item);
    t.key_len = item->key_len - 2;
    t.key = item->data + 2;
    t.value_len = item->data_len;
    t.value = item->data + item->key_len;
    mem_cache::item_dump.dump(&t);
    return;
}

}                                /* ns tair */
