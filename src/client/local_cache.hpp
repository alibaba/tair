#ifndef LOCAL_CACHE_HPP
#define LOCAL_CACHE_HPP

#include "local_cache.h"

namespace tair 
{

#define LOCAL_CACHE_TEMPLATE template<class KeyT,  \
                                      class ValueT,\
                                      class Hash,  \
                                      class Pred>

#define LOCAL_CACHE_CLASS local_cache<KeyT, ValueT, Hash, Pred>


LOCAL_CACHE_TEMPLATE
LOCAL_CACHE_CLASS::~local_cache() 
{
  clear();
}

LOCAL_CACHE_TEMPLATE
LOCAL_CACHE_CLASS::local_cache(size_t cap, int64_t exp) : 
    capability(cap > 0 ? cap : 0),
    expire(exp)
{
  tbsys::CThreadGuard guard(&mutex);
  lru.clear();
  cache.clear();
}

LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::touch(const KeyT& key) 
{
  // lock 
  tbsys::CThreadGuard guard(&mutex);
  // find
  entry_cache_iterator iter = cache.find(&key);
  if (iter == cache.end()) {
    // miss
    return ;
  }
  // update utime
  set_entry_utime_now(iter->second);
}


LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::remove(const KeyT& key)
{
  tbsys::CThreadGuard guard(&mutex);
  entry_cache_iterator iter = cache.find(&key);
  if (iter == cache.end()) {
    return ;
  }
  lru.erase(iter->second);
  cache.erase(iter);
}

LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::put(const KeyT& key, const ValueT& val) 
{
  if (capability < 1) {
      return ;
  }
  // lock 
  tbsys::CThreadGuard guard(&mutex);

  entry_cache_iterator iter = cache.find(&key);
  internal_entry *entry = NULL;
  if (iter == cache.end()) {
    // not found
    // evict entry
    while (cache.size() >= capability) {
      assert(capability >= 1);
      const internal_entry *evict = evict_one();
      assert(evict != NULL);
      // free entry
      drop_entry(evict);
    }

    // insert new one 
    // allocate internal_entry, relase after evict or in clear()
    entry = make_entry(key, val);
    if (entry == NULL) {
      return ;
    }
    lru.push_front(entry);   
  } else {
    // found, already exists
    // adjust lru list
    lru.splice(lru.begin(), lru, iter->second); 
    // update entry value and utime
    // update first, some meta info
    cache.erase(iter);
    entry_list_iterator elem = lru.begin();
    set_entry_key(elem, key);
    set_entry_value(elem, val);
    set_entry_utime_now(elem);
    entry = *elem;
  }
  cache[&(entry->key)] = lru.begin();
}

LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::clear()
{
  // lock 
  tbsys::CThreadGuard guard(&mutex);
  // evict every entry
  while (cache.size() > 0) {
    const internal_entry *evict = evict_one();
    assert(evict != NULL);
    // free entry
    drop_entry(evict);
  }
  lru.clear();
  cache.clear();
}


LOCAL_CACHE_TEMPLATE
typename LOCAL_CACHE_CLASS::result 
LOCAL_CACHE_CLASS::get(const KeyT& key, ValueT& value) 
{
  // lock
  if (capability < 1) {
    return MISS;
  }
  tbsys::CThreadGuard guard(&mutex);

  entry_cache_iterator iter = cache.find(&key);
  if (iter == cache.end()) {
    return MISS;
  }
  // adjust lru list
  lru.splice(lru.begin(), lru, iter->second); 
  // whatever, find entry, fill value
  fill_value(iter, value);
  // check whether entry was expired 
  int64_t now = tbutil::Time::now().toMilliSeconds();
  if (expire != 0 && now - entry_utime(iter) > expire) {
    // expired
    set_entry_utime(iter->second, now);
    return EXPIRED;
  } else {
    // fresh entry
    return HIT;
  }
}

#undef LOCAL_CACHE_TEMPLATE 
#undef LOCAL_CACHE_CLASS

}

#endif
