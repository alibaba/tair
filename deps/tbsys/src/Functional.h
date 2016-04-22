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

#ifndef TBSYS_FUNCTIONAL_H
#define TBSYS_FUNCTIONAL_H

#include "Handle.h"
#include <functional>

namespace tbutil
{
template<class R, class T, class H>
class ConstMemFun : public std::unary_function<H, R>
{
    typedef R (T::*MemberFN)(void) const;
    MemberFN _mfn;

public:

    explicit ConstMemFun(MemberFN p) : _mfn(p) { }
    R operator()(H handle) const
    {
        return (handle.get() ->* _mfn)();
    }
};

template<class R, class T>
inline ConstMemFun<R, T, Handle<T> >
constMemFun(R (T::*p)(void) const)
{
    return ConstMemFun<R, T, Handle<T> >(p);
}
}//end namespace
#endif
