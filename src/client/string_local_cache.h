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

#ifndef STRING_LOCAL_CACHE_H
#define STRING_LOCAL_CACHE_H

#include "local_cache.hpp"

namespace tair {

typedef local_cache<std::string,
        std::string,
        std::tr1::hash<std::string>,
        std::equal_to<std::string> >
        string_local_cache;
};


#endif

