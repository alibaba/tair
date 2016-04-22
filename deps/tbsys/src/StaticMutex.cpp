/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#include "StaticMutex.h"
tbutil::StaticMutex tbutil::globalMutex = TNET_STATIC_MUTEX_INITIALIZER;
