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
#include "mem_cache.hpp"
#include "tblog.h"
#include "mdb_manager.hpp"
#include <string.h>

namespace tair {

  data_dumpper mem_cache::item_dump;

  bool mem_cache::initialize(int max_slab_id, int base_size, float factor)
  {
    assert(this_mem_pool != 0 && max_slab_id > 0 && base_size > 0);
    cache_info =
      reinterpret_cast <
      mdb_cache_info * >(this_mem_pool->get_pool_addr() +
                         mem_pool::MEM_POOL_METADATA_LEN);
    if(cache_info->inited != 1)
    {
      cache_info->max_slab_id = max_slab_id;
      cache_info->base_size = base_size;
      cache_info->factor = factor;
      slab_initialize();
    }
    else
    {
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

  mdb_item *mem_cache::alloc_item(int size, int &type)
  {
    slab_manager *slabmng = get_slabmng(size);
    if(slabmng == 0)
      return 0;
    TBSYS_LOG(DEBUG,"size:%d,slabmng.slab_size : %d",size,slabmng->slab_size);
    return slabmng->alloc_item(type);
  }

  void mem_cache::update_item(mdb_item * item)
  {
    slab_managers[SLAB_ID(item->item_id)]->update_item(item, ITEM_AREA(item));
  }

  void mem_cache::free_item(mdb_item * item)
  {
    if(item == 0)
      return;
    slab_manager *slabmng = slab_managers.at(SLAB_ID(item->item_id));        //TODO catch exception
    assert(slabmng != 0);
    slabmng->free_item(item);
  }

  int mem_cache::free_page(int slab_id)
  {
    assert(slab_id >= 0 && slab_id < get_slabs_count());

    slab_manager *slab_mng = slab_managers.at(slab_id);
    assert(slab_mng != 0);

    if((slab_mng->free_pages_no + slab_mng->full_pages_no
        + slab_mng->partial_pages_no) <= 1) {
      return -1;                //just have 1 page,we can't free the only page of this slab
    }
    page_info *info = 0;

    if(slab_mng->free_pages_no > 0) {
      goto FREE_PAGE;
    }
    else if(slab_mng->partial_pages_no > 0) {
      info = slab_mng->PAGE_INFO(this_mem_pool-> index_to_page(
                  slab_mng-> get_the_most_free_items_of_partial_page_id ()));
    }
    else if(slab_mng->full_pages_no > 0) {
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

  int mem_cache::get_slabs_count()
  {
    return cache_info->max_slab_id + 1;
  }

  bool mem_cache::is_quota_exceed(int area)
  {
    //return false; //for debug
    return manager->is_quota_exceed(area);
  }

  void mem_cache::calc_slab_balance_info(std::map<int, int> &adjust_info)
  {
    double crrnt_no = 0.0;
    double should_have_no = 0.0;

    double evict_ratio;

    int need_alloc_page = 0;

    adjust_info.clear();

    for(vector<slab_manager *>::iterator it = slab_managers.begin();
        it != slab_managers.end(); ++it) {
      if((*it)->item_total_count == 0) {        //
        continue;
      }
      crrnt_no += (*it)->item_total_count;
      should_have_no += ((*it)->item_total_count + (*it)->evict_total_count);
    }

    if(should_have_no == 0 || crrnt_no == 0) {
      //don't worry about "float compare"
      return;                        //just in case
    }

    evict_ratio = should_have_no / crrnt_no;

    TBSYS_LOG(WARN, "should_total:%f,crrnt_no:%f,evict_ratio:%f",
              should_have_no, crrnt_no, evict_ratio);

    for(vector<slab_manager *>::iterator it = slab_managers.begin();
        it != slab_managers.end(); ++it) {
      if((*it)->item_total_count == 0) {
        continue;
      }
      should_have_no = ((*it)->item_total_count + (*it)->evict_total_count);
      int final_count = static_cast<int>(should_have_no / evict_ratio);
      int need_count = final_count - (*it)->item_total_count;

      int need_page =
        (::abs(need_count) + (*it)->per_slab - 1) / (*it)->per_slab;
      need_page = need_count > 0 ? need_page : -need_page;

      TBSYS_LOG(WARN,
                "now:%lu,evict:%lu,should:%f,final_count:%d,need_count:%d,need_page:%d",
                (*it)->item_total_count, (*it)->evict_total_count,
                should_have_no, final_count, need_count, need_page);

      adjust_info.insert(make_pair<int, int>((*it)->slab_id, need_page));

      need_alloc_page += need_count > 0 ? need_page : 0;
    }

    if(need_alloc_page <= this_mem_pool->get_free_pages_num()) {
      for(map<int, int>::iterator it = adjust_info.begin();
          it != adjust_info.end(); ++it) {
        if(it->second < 0) {
          it->second = 0;
        }
      }

    }
    for(map<int, int>::iterator it = adjust_info.begin();
        it != adjust_info.end(); ++it) {
      TBSYS_LOG(WARN, " (%d) : %d", it->first, it->second);
    }

    return;

  }

  void mem_cache::balance_slab_done()
  {
    for(vector<slab_manager *>::iterator it = slab_managers.begin();
        it != slab_managers.end(); ++it) {
      (*it)->evict_total_count = 0;
    }
    return;
  }

//TODO
  void mem_cache::keep_area_quota(int area, int exceed)
  {
    assert(area >= 0 && area < TAIR_MAX_AREA_COUNT);
    assert(exceed > 0);

    int exceed_bytes = exceed;
    bool first_round = true;

    while(exceed_bytes > 0) {

      for(vector<slab_manager *>::iterator it = slab_managers.begin();
          it != slab_managers.end(); ++it) {
        if((*it)->this_item_list[area].item_head == 0
           || (*it)->this_item_list[area].item_tail == 0) {
          // this slab have no mdb_item of this area
          continue;
        }
        if((*it)->item_count[area] < static_cast<uint32_t> ((*it)->per_slab) && first_round) {        //lower than one page
          TBSYS_LOG(DEBUG, "just one page");
          continue;
        }

        //ok,we will delete one mdb_item
        mdb_item *item = id_to_item((*it)->this_item_list[area].item_tail);
        exceed_bytes -= (item->key_len + item->data_len);
        manager->__remove(item);

        if(exceed_bytes <= 0) {
          break;
        }
      }

      first_round = false;
    }

  }

  uint64_t mem_cache::get_item_head(int slab_id, int area)
  {
    assert(slab_id < get_slabs_count());
    assert(area < TAIR_MAX_AREA_COUNT);
    slab_manager *slab_mng = slab_managers.at(slab_id);
    assert(slab_mng != 0);
    return slab_mng->get_item_head(area);
  }

  void mem_cache::display_statics()
  {
    TBSYS_LOG(WARN, "total slab : %u", slab_managers.size());

    for(vector<slab_manager *>::iterator it = slab_managers.begin();
        it != slab_managers.end(); ++it) {
      (*it)->display_statics();
    }
  }


  mem_cache::slab_manager * mem_cache::get_slabmng(int size)
  {
    std::vector<slab_manager *>::iterator it = slab_managers.begin();
    for(; it != slab_managers.end(); ++it) {
      if((*it)->slab_size >= size)        //found
        break;
    }
    return it != slab_managers.end()? (*it) : 0;
  }

  bool mem_cache::slab_initialize()
  {
    if(cache_info->base_size < (int) (sizeof(mdb_item) + 16)) {
      cache_info->base_size = ALIGN(sizeof(mdb_item) + 16);        /* at least 16 bytes */
    }
    char *slab_start_addr =
      this_mem_pool->get_pool_addr() + mem_pool::MEM_POOL_METADATA_LEN +
      sizeof(mdb_cache_info);

    TBSYS_LOG(DEBUG, "pool:%p,slab_start:%p", this_mem_pool->get_pool_addr(),
              slab_start_addr);

    int page_size = this_mem_pool->get_page_size();
    assert(cache_info->base_size < page_size);

    int start = cache_info->base_size;
    int end = page_size - sizeof(page_info);

    int total_size = 0;
    char *next_slab_addr = slab_start_addr;
    int i = 0;
    for(; i < cache_info->max_slab_id && start <= end; ++i) {

      if(start > static_cast<int>(end / 2)) {        //page_size slab
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
      slabmng->page_size = page_size;
      slabmng->evict_index = 0;
      slabmng->evict_total_count = 0;
      slabmng->item_total_count = 0;
      slabmng->partial_pages =
        reinterpret_cast< uint32_t *>(next_slab_addr + sizeof(slab_manager));

      TBSYS_LOG(DEBUG, "slab: id:%d,size:%d,per_slab:%d", i, start,
                slabmng->per_slab);
      //prealloc
      slabmng->pre_alloc(1);

      //next slab
      next_slab_addr +=
        slabmng->partial_pages_bucket_no() * sizeof(uint32_t) +
        sizeof(slab_manager);
      start = ALIGN((int) (start * cache_info->factor));
      slab_managers.push_back(slabmng);
      total_size += sizeof(slab_manager);
      total_size += slabmng->partial_pages_bucket_no() * sizeof(uint32_t);

    }
    assert(total_size < mem_cache::MEMCACHE_META_LEN);
    TBSYS_LOG(DEBUG, "total_size:%d,meta:%d", total_size,
              mem_cache::MEMCACHE_META_LEN);
    cache_info->max_slab_id = i - 1;
    /*TODO last slab (page size) */
    return true;
  }
  bool mem_cache::get_slab_info()
  {
    char *next_slab_addr =
      this_mem_pool->get_pool_addr() + mem_pool::MEM_POOL_METADATA_LEN +
      sizeof(mdb_cache_info);
    for(int i = 0; i <= cache_info->max_slab_id; ++i) {
      slab_manager *slabmng = reinterpret_cast<slab_manager *>(next_slab_addr);
      slabmng->this_mem_pool = this_mem_pool;
      slabmng->cache = this;
      slabmng->partial_pages =reinterpret_cast< uint32_t *>(next_slab_addr + sizeof(slab_manager));
      slab_managers.push_back(slabmng);
      next_slab_addr += slabmng->partial_pages_bucket_no() * sizeof(uint32_t) +
        sizeof(slab_manager);
    }
    return true;
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

  mdb_item *mem_cache::slab_manager::alloc_new_item(int area)
  {
    char *page = 0;
    mdb_item *item = 0;
    uint64_t item_id = 0;
    page_info *info = 0;

    TBSYS_LOG(DEBUG, "alloc_new_item,area : %d", area);
    if(partial_pages_no > 0) {
      TBSYS_LOG(DEBUG, "alloc from partial page:%u", get_partial_page_id());
      assert(get_partial_page_id() > 0);
      info = PAGE_INFO(this_mem_pool->index_to_page(get_partial_page_id()));
      assert(info != 0);
      assert(info->free_nr > 0);
      assert(info->free_head != 0);
      assert(info->free_nr <= per_slab);

      item_id = info->free_head;
      unlink_page(info, partial_pages[info->free_nr / PARTIAL_PAGE_BUCKET]);
    }
    else if(free_pages_no > 0) {
      TBSYS_LOG(DEBUG, "alloc from free page : %u", free_pages);
      info = PAGE_INFO(this_mem_pool->index_to_page(free_pages));
      assert(info != 0);
      assert(info->free_nr == per_slab);
      assert(info->free_head != 0);
      item_id = info->free_head;
      unlink_page(info, free_pages);
      --free_pages_no;
      ++partial_pages_no;

    }
    else {
      //allocate a new page
      TBSYS_LOG(DEBUG, "allocate a new page");
      int index;
      page = this_mem_pool->alloc_page(index);
      if(page == 0) {
        TBSYS_LOG(DEBUG, "allocate a new page falied");
        return 0;
      }
      init_page(page, index);
      info = PAGE_INFO(page);
      assert(info != 0);
      item_id = info->free_head;
      ++partial_pages_no;
    }

    //TODO(maoqi),CLEAR_FLAGS(item_id); 

    //page : m_free_pages -> m_partial_pages -> m_full_pages
    //every page will obey this rule,even though the page that has only one item.

    --(info->free_nr);
    if(info->free_nr == 0) {
      link_page(info, full_pages);
      ++full_pages_no;
      --partial_pages_no;
      info->free_head = 0;
    }
    else {
      TBSYS_LOG(DEBUG, "link into partial_pages[%d]",
                info->free_nr / PARTIAL_PAGE_BUCKET);
      link_page(info, partial_pages[info->free_nr / PARTIAL_PAGE_BUCKET]);
    }

    assert(item_id > 0);

    ++item_total_count;
    item = id_to_item(item_id);
    assert(item != 0);

   
    info->free_head = item->h_next;
    
    TBSYS_LOG(DEBUG, "from page id:%u,now free:%d,free-head:%lu,id will be use:%lu", info->id, info->free_nr,info->free_head,item_id);
   
    link_item(item, area);

    if (item->item_id != item_id)
    {
      TBSYS_LOG(ERROR,"why? overwrite? item->item_id [%lu],item_id[%lu]",item->item_id,item_id);
      assert(0);
    }
    
    if (info->free_nr != 0 && info->free_head == 0)
    {
      TBSYS_LOG(ERROR,"the free nr of page is greater than 0,buf the free_head is zero");
      TBSYS_LOG(ERROR,"item:%p,page:%p",item,info);
      assert(0);
    }else if (info->free_nr == 0)
    {
      assert(info->free_head == 0);
    }
    else
    {
      if (PAGE_ID((info->free_head)) != info->id)
      {
        TBSYS_LOG(ERROR,"the page id of free_head [%d] != info->id [%d]",PAGE_ID(info->free_head),info->id);
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
  mdb_item *mem_cache::slab_manager::alloc_item(int &type)
  {
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
    TBSYS_LOG(DEBUG,"alloc_item,area:%d",area);
    assert(area < TAIR_MAX_AREA_COUNT);
    bool exceed = cache->is_quota_exceed(area);

    if(!exceed) {
      goto ALLOC_NEW;
    }
    else {
      TBSYS_LOG(DEBUG, "quota is exceed,evict_self");
      goto EVICT_SELF;
    }
  ALLOC_NEW:
    it = alloc_new_item(area);
    if(it == 0 && !exceed) {
      goto EVICT_SELF;
    }
    else if(it == 0) {
      goto EVICT_ANY;
    }
    type = ALLOC_NEW;

    /*Stat info */
    ++item_count[area];
    return it;
  EVICT_SELF:
    it = evict_self(type);
    if(it == 0 && !exceed) {
      goto EVICT_ANY;
    }
    else if(it == 0) {
      goto ALLOC_NEW;
    }

    if(type == ALLOC_EVICT_SELF && !exceed) {
      ++evict_total_count;
      ++evict_count[area];
    }
    return it;
  EVICT_ANY:
    it = evict_any(type);
    assert(it != 0);

    if(type == ALLOC_EVICT_ANY) {
      ++evict_total_count;
      ++evict_count[ITEM_AREA(it)];
    }
    --item_count[ITEM_AREA(it)];
    ++item_count[area];
    return it;
  }

  void mem_cache::slab_manager::update_item(mdb_item * item, int area)
  {
    unlink_item(item);
    link_item(item, area);
  }

  void mem_cache::slab_manager::free_item(mdb_item * item)
  {
    assert(item != 0 && item->item_id != 0);
    --item_total_count;
    unlink_item(item);
    page_info *info =
      PAGE_INFO(this_mem_pool->index_to_page(PAGE_ID(item->item_id)));
    if(info->free_nr == 0) {
      unlink_page(info, full_pages);
      --full_pages_no;
      ++partial_pages_no;
    }
    else {
      assert(info->free_head != 0);
      unlink_page(info, partial_pages[info->free_nr / PARTIAL_PAGE_BUCKET]);
    }

    item->prev = 0;
    item->next = 0;

    TBSYS_LOG(DEBUG,
              "id:%lu before free:page id:%d,%p,info->free_nr:%d,info->free_head:%lu,SLAB_ID(id):%d,",
              item->item_id,info->id,info, info->free_nr, info->free_head, SLAB_ID(item->item_id));

    item->h_next = info->free_head;
    info->free_head = item->item_id;
    

    if(++info->free_nr == per_slab) {
      TBSYS_LOG(DEBUG, "this page is all free:%d,free_nr:%d", info->id, info->free_nr);
      --partial_pages_no;
      
      if(free_pages_no + full_pages_no + partial_pages_no <= 1) {
        TBSYS_LOG(DEBUG, "only one page,don't free");
        link_page(info, free_pages);
        ++free_pages_no;
      }else {
        TBSYS_LOG(DEBUG, "send this page to pool");
        this_mem_pool->free_page(info->id);
      }
    }
    else {
      link_page(info, partial_pages[info->free_nr / PARTIAL_PAGE_BUCKET]);
    }
 
    TBSYS_LOG(DEBUG, "after free:page id:%d,%p,info->free_nr:%d,info->free_head:%lu,SLAB_ID(id):%d,info->free_head->next : %lu",
        info->id,info,info->free_nr,info->free_head,SLAB_ID(item->item_id),item->h_next);
  }


  mdb_item *mem_cache::slab_manager::evict_self(int &type)
  {
    uint32_t crrnt_time = time(NULL);
    int times = 50;
    mdb_item *item = 0;
    item_list *head = &this_item_list[type];
    int area = type;
    TBSYS_LOG(DEBUG, "evict_self,area:%d", type);
    uint64_t pos = head->item_tail;
    TBSYS_LOG(DEBUG, "tail:%lu", pos);

    if(pos == 0) {                // this area have no item of this slab
      TBSYS_LOG(DEBUG, "this area have no item of this size,area:%d,size:%d",
                type, slab_size);
      return 0;
    }
    bool found = false;
    while(times-- > 0 && pos != 0) {
      item = id_to_item(pos);
      assert(item != 0);
      if(item->exptime > 0 && item->exptime < crrnt_time) {
        type = ALLOC_EXPIRED;
        found = true;
        break;
      }
      pos = item->prev;
    }
    if(!found) {
      type = ALLOC_EVICT_SELF;
      pos = head->item_tail;
      item = id_to_item(pos);
      if (ITEM_AREA(item) != area)
      {
        TBSYS_LOG(ERROR,"item in [%d] list is not my item [%d]",area,ITEM_AREA(item));
        assert(0);
      }
      dump_item(item);
    }
    CLEAR_FLAGS(item->item_id);
    update_item(item, ITEM_AREA(item));
    return item;
  }

/*
 * called after alloc_item or evict_self
 */

  mdb_item *mem_cache::slab_manager::evict_any(int &type)
  {
    assert(type < TAIR_MAX_AREA_COUNT);
    if(evict_index >= TAIR_MAX_AREA_COUNT) {
      evict_index = 0;
    }
    TBSYS_LOG(INFO,"evict any,area:%d",type);
    uint32_t crrnt_time = time(NULL);
    mdb_item *item = 0;
    int times = 0;
    int area = type;
    type = ALLOC_EVICT_ANY;
    while(evict_index < TAIR_MAX_AREA_COUNT) {
      item_list *head = &this_item_list[evict_index];
      uint64_t pos = head->item_tail;

      if(pos != 0) {
        item = id_to_item(pos);
        if(item->exptime > 0 && item->exptime < crrnt_time) {
          type = ALLOC_EXPIRED;        //
        }
        break;
      }

      if(++evict_index >= TAIR_MAX_AREA_COUNT) {
        evict_index = 0;
      }

      if(++times > TAIR_MAX_AREA_COUNT) {
        assert(false);                //shoudn't be here,just in case
      }
    }
    CLEAR_FLAGS(item->item_id);
    update_item(item, area);
    return item;
  }

  void mem_cache::slab_manager::init_page(char *page, int index)
  {

    memset(page, 0, page_size);

    page_info *info = PAGE_INFO(page);
    info->id = index;
    info->free_nr = per_slab;
    info->free_head = ITEM_ID(slab_size, 0, index, slab_id);

    TBSYS_LOG(DEBUG, "new page:%p,slab_size:%d,free_nr:%d,m_id:%d,free_head:%lu",page ,slab_size,
              per_slab, index,info->free_head);

    char *item_start = page + sizeof(page_info);
    mdb_item *item = 0;
    for(int i = 0; i < per_slab; ++i) {
      item = reinterpret_cast<mdb_item *>(item_start + i * slab_size);
      item->next = item->prev = 0;
      item->item_id = ITEM_ID(slab_size, i, index, slab_id);
      item->h_next = ITEM_ID(slab_size, i + 1, index, slab_id);
    }
    item->h_next = 0;

    for(int i=0; i< per_slab;++i)
    {
      item = reinterpret_cast<mdb_item *>(item_start + i * slab_size);
      TBSYS_LOG(DEBUG,"in page [%d],the %dth item: %lu,h_next :%lu",info->id,i,item->item_id,item->h_next);
    }
  }

  void mem_cache::clear_page(slab_manager * slab_mng, char *page)
  {

    //page_info *info = slabmng->PAGE_INFO(page);
    char *item_start = page + sizeof(page_info);
    mdb_item *item = 0;
    for(int i = 0; i < slab_mng->per_slab; ++i) {
      item =
        reinterpret_cast<mdb_item *>(item_start + i * slab_mng->slab_size);
      if(item->prev == 0 && item->next == 0) {
        continue;                //is free
      }
      manager->__remove(item);
    }
  }

  void mem_cache::slab_manager::link_item(mdb_item * item, int area)
  {
    assert(item != 0);
    assert(item->item_id != 0);
    assert(area < TAIR_MAX_AREA_COUNT);
    item_list *head = &this_item_list[area];
    TBSYS_LOG(DEBUG,"link_item [%p] [%lu] into [%d] [%p],list",item,item->item_id,area,head);

    item->next = head->item_head;
    item->prev = 0;
    if(item->next != 0) {
      mdb_item *old_head = id_to_item(item->next);
      old_head->prev = item->item_id;
    }
    head->item_head = item->item_id;

    if(head->item_tail == 0) {
      head->item_tail = item->item_id;
    }
  }

  void mem_cache::slab_manager::unlink_item(mdb_item * item)
  {
    assert(item != 0);
    assert(item->item_id != 0);
    assert(ITEM_AREA(item) < TAIR_MAX_AREA_COUNT);

    mdb_item *prev = 0;
    mdb_item *next = 0;

    item_list *head = &this_item_list[ITEM_AREA(item)];

    TBSYS_LOG(DEBUG,"unlink item [%p] [%lu] from [%d] [%p],list",item,item->item_id,ITEM_AREA(item),head);

    if(itemid_equal(item->item_id, head->item_head)) {
      head->item_head = item->next;
    }

    if(itemid_equal(item->item_id, head->item_tail)) {
      head->item_tail = item->prev;
    }

    if(item->prev != 0) {
      prev = id_to_item(item->prev);
      prev->next = item->next;
    }

    if(item->next != 0) {
      next = id_to_item(item->next);
      next->prev = item->prev;
    }
  }

  void mem_cache::slab_manager::link_page(page_info * info,
                                          uint32_t & page_head)
  {
    assert(info != 0);
    info->prev = 0;
    info->next = page_head;
    if(info->next != 0) {
      page_info *next_pageinfo =
        PAGE_INFO(this_mem_pool->index_to_page(info->next));
      next_pageinfo->prev = info->id;
    }
    page_head = info->id;
  }
  void mem_cache::slab_manager::unlink_page(page_info * info,
                                            uint32_t & page_head)
  {
    assert(info != 0);
    if(info->id == page_head) {
      page_head = info->next;
    }
    if(info->next) {
      page_info *next_pageinfo =
        PAGE_INFO(this_mem_pool->index_to_page(info->next));
      next_pageinfo->prev = info->prev;
    }
    if(info->prev) {
      page_info *prev_pageinfo =
        PAGE_INFO(this_mem_pool->index_to_page(info->prev));
      prev_pageinfo->next = info->next;
    }
    info->prev = info->next = 0;
  }

  int mem_cache::slab_manager::pre_alloc(int pages)
  {
    assert(pages > 0);
    int index = 0;
    char *page = 0;
    page_info *info = 0;

    int allocated = 0;
    for(int i = 0; i < pages; ++i) {
      page = this_mem_pool->alloc_page(index);
      if(page == 0) {
        TBSYS_LOG(DEBUG, "allocate page falied");
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

  void mem_cache::slab_manager::display_statics()
  {
    TBSYS_LOG(DEBUG, "slab_id:%u,slab_size:%d,per_slab:%d\n", slab_id,
              slab_size, per_slab);
    TBSYS_LOG(DEBUG,
              "item_total_count:%lu,full_pages_no:%d,partial_pages_no:%d,free_pages_no:%d\n",
              item_total_count, full_pages_no, partial_pages_no,
              free_pages_no);
  }

  void mem_cache::slab_manager::dump_item(mdb_item * item)
  {
    if(item == 0) {
      return;
    }
    TBSYS_LOG(DEBUG, "\033[;31mevict data,start dump ...\033[0;m");
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
