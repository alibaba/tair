/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  WarningBuffer.cpp,  09/27/2012 05:01:00 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh
 * Description:  
 *   fix length string ring buffer
 * 
 */
#include "WarningBuffer.h"

namespace tbsys {
  bool WarningBuffer::is_log_on_ = false;
  
  WarningBuffer *get_tsi_warning_buffer()
  {
    static WarningBufferFactory instance;
    return instance.get_buffer();
  }


};

