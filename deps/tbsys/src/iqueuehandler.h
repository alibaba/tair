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

#ifndef TBSYS_QUEUE_HANDLER_H_
#define TBSYS_QUEUE_HANDLER_H_

namespace tbsys {

class IQueueHandler {
public:    
    virtual ~IQueueHandler() {}
    virtual bool handleQueue(void *data, int len, int threadIndex, void *args) = 0;
};

}

#endif /*TBSYS_QUEUE_HANDLER_H_*/
