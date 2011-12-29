#ifndef DATA_ENTRY_LOCAL_CACHE_H
#define DATA_ENTRY_LOCAL_CACHE_H

#include "local_cache.hpp"
#include "data_entry.hpp" 

namespace tair
{

struct data_entry_hash 
{
  size_t operator()(const tair::common::data_entry& key) const
  {
    tair::common::data_entry *entry = 
          const_cast<tair::common::data_entry *>(&key);
    uint64_t code = entry->get_hashcode();
    return static_cast<size_t>(code);
  }
};

struct data_entry_equal_to
{
  bool operator()(const tair::common::data_entry &x, 
                  const tair::common::data_entry &y) const
  {
    return x == y; 
  }
};
  
typedef local_cache<tair::common::data_entry,
                    tair::common::data_entry,
                    tair::data_entry_hash,
                    tair::data_entry_equal_to > data_entry_local_cache;
};




#endif
   
