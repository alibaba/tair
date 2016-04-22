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

#include <cassert>
#include "Shared.h"

namespace tbutil 
{
SimpleShared::SimpleShared() :
    _ref(0),
    _noDelete(false)
{
}

SimpleShared::SimpleShared(const SimpleShared&) :
    _ref(0),
    _noDelete(false)
{
}

Shared::Shared() :
    _ref(0),
    _noDelete(false)
{
}

Shared::Shared(const Shared&) :
    _ref(0),
    _noDelete(false)
{
}

void Shared::__incRef()
{
    _mutex.lock();
    ++_ref;
    _mutex.unlock();
}

void Shared::__decRef()
{
    _mutex.lock();
    bool doDelete = false;
    assert(_ref > 0);
    if(--_ref == 0)
    {
        doDelete = !_noDelete;
        _noDelete = true;
    }
    _mutex.unlock();
    if(doDelete)
    {
        delete this;
    }
}

int Shared::__getRef() const
{
    _mutex.lock();
    const int ref = _ref;
    _mutex.unlock();
    return ref;
}

void Shared::__setNoDelete(bool b)
{
    _noDelete = b;
}
}//end namespace tbutil
