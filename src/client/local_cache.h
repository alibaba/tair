/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   YeXiang <yexiang.ych@taobao.com>
 *
 */
#ifndef LOCAL_CACHE_H
#define LOCAL_CACHE_H

#include <list>
#include <tr1/unordered_map>

#include "Time.h"
#include "threadmutex.h"

namespace tair 
{

template<typename KeyT, 
         typename ValueT, 
         typename Hash = std::tr1::hash<KeyT>, 
         typename Pred = std::equal_to<KeyT> > 
class local_cache 
{
public:

  // cap: cache max size, if cache size reach cap, lru one entry
  // expire: entry expire time, refresh interval
  local_cache(size_t cap);
  virtual ~local_cache();

  typedef enum {
    HIT,
    MISS,
    EXPIRED,
    SETOK,
    SETERROR
  } result;
 
  // put a entry in cache, 
  // if size >= capacity, evict entry, until size < capabiliy
  virtual void put(const KeyT& key, const ValueT& val); 

  // update entry utime
  virtual void touch(const KeyT& key);

  // HIT: found
  // MISS: not found
  // EXPIRED: found, but expired, 
  // the one who call this function must upate the value. 
  // update utime while returning EXPIRED for preventing
  // some threads concurrent update the entry.
  virtual result get(const KeyT& key, ValueT& val);

  virtual void remove(const KeyT& key);

  virtual void clear();

  size_t size() 
  {
    tbsys::CThreadGuard guard(&mutex);
    return cache.size();
  }

  size_t get_capacity() 
  {
    return this->capacity;  
  }

  void set_capacity(size_t cap) 
  {
    this->capacity = cap > 0 ? cap : 0;
    if(cap < 1) {
      clear();
    } else {
      tbsys::CThreadGuard guard(&mutex);
      while (cache.size() >= capacity) {
        assert(capacity >= 1);
        const internal_entry *evict = evict_one();
        assert(evict != NULL);
        // free entry
        drop_entry(evict);
      }
    }
  }

  result set_expire(uint64_t e)
  {
    if (e > 0) {
      this->expire = e;
      return SETOK;
    } else {
      return SETERROR;
    }
  }

protected:

  size_t lru_size() {
    return lru.size();
  }

  typedef struct internal_entry
  {
    KeyT    key;
    ValueT  value;
    uint64_t utime;

    internal_entry(const KeyT& key, const ValueT& value, uint64_t utime)
      : key(key), value(value), utime(utime) {}
  };

  struct hash_wrapper 
  {
    size_t operator()(const KeyT *key) const
    {
      return hash_impl(*key);
    }
  private:
    Hash hash_impl;
  };

  struct equal_to_wrapper
  {
    bool operator()(const KeyT *x, const KeyT *y) const
    {
      return equal_to_impl(*x, *y);
    }
  private:
    Pred equal_to_impl;
  };

  typedef std::list<internal_entry *> entry_list;
  typedef typename entry_list::iterator entry_list_iterator;
  typedef std::tr1::unordered_map<const KeyT *, 
                                  entry_list_iterator, 
                                  hash_wrapper, 
                                  equal_to_wrapper> entry_cache;
  typedef typename entry_cache::iterator entry_cache_iterator;

  const internal_entry* evict_one() 
  {
    // erase last element
    if (lru.empty())
      return NULL;
    const internal_entry * last_elem = lru.back();    
    cache.erase(&(last_elem->key));
    lru.pop_back();
    return last_elem;
  }
private:
  uint64_t entry_utime(const entry_cache_iterator& iter)
  {
    return (*(iter->second))->utime;
  }

  void set_entry_key(const entry_list_iterator& iter, const KeyT& key)
  {
    (*iter)->key = key;
  }

  void set_entry_utime(const entry_list_iterator& iter, int64_t utime)
  {
    (*iter)->utime = utime;
  }

  void set_entry_value(const entry_list_iterator& iter, const ValueT& value)
  {
    (*iter)->value = value;
  }

  void fill_value(const entry_cache_iterator& iter, ValueT& value)
  {
    value = (*(iter->second))->value;
  }

  void set_entry_utime_now(const entry_list_iterator& iter)
  {
    set_entry_utime(iter, 
        tbutil::Time::now().toMilliSeconds());
  }

  internal_entry* make_entry(const KeyT& key, const ValueT& val)
  {
    int64_t now = tbutil::Time::now().toMilliSeconds();
    internal_entry *entry = new(std::nothrow) internal_entry(
        key, val, now);
    if (entry == NULL) {
      //TODO: log 
    }
    return entry;
  }

  void drop_entry(const internal_entry *entry) 
  {
    delete entry;
  }
  
private:
  entry_list lru;
  entry_cache cache;

  size_t capacity;
  uint64_t expire;

  tbsys::CThreadMutex mutex;
};

} // end namespace

#endif

