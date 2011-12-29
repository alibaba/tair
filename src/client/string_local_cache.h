#ifndef STRING_LOCAL_CACHE_H
#define STRING_LOCAL_CACHE_H

#include "local_cache.hpp"

namespace tair
{

typedef local_cache<std::string, 
                    std::string,
                    std::tr1::hash<std::string>,
                    std::equal_to<std::string> > 
                    string_local_cache;
};




#endif

