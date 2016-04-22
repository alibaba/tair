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

#ifndef TAIR_LOG_H
#define TAIR_LOG_H

#include <tbsys.h>

namespace tair {
#define log_error(...) TBSYS_LOG(ERROR, __VA_ARGS__)
#define log_warn(...) TBSYS_LOG(WARN, __VA_ARGS__)
#define log_info(...) TBSYS_LOG(INFO, __VA_ARGS__)
#define log_debug(...) TBSYS_LOG(DEBUG, __VA_ARGS__)
}

#endif
