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

#ifndef TBSYS_SHARED_H 
#define TBSYS_SHARED_H 
#include "Mutex.h"
namespace tbutil
{
/** 
* @brief SimpleShared类提供简单的引用计数
*/
class SimpleShared
{
public:

    SimpleShared();
    SimpleShared(const SimpleShared&);

    virtual ~SimpleShared()
    {
    }

    SimpleShared& operator=(const SimpleShared&)
    {
        return *this;
    }

    void __incRef()
    {
        assert(_ref >= 0);
        ++_ref;
    }

    void __decRef()
    {
        assert(_ref > 0);
        if(--_ref == 0)
        {
            if(!_noDelete)
            {
                _noDelete = true;
                delete this;
            }
        }
    }

    int __getRef() const
    {
        return _ref;
    }

    void __setNoDelete(bool b)
    {
        _noDelete = b;
    }

private:

    int _ref;
    bool _noDelete;
};

/** 
 * @brief Shared 提供简单的引用计数,主要用于智能指针
 * 如果要用于智能指针，用户的类需要继承此类 
 */
class Shared
{
public:

    Shared();
    Shared(const Shared&);

    virtual ~Shared()
    {
    }

    Shared& operator=(const Shared&)
    {
        return *this;
    }

    /** 
     * @brief 增加引用计数
     */
    virtual void __incRef();
    /** 
     * @brief 减少引用计数
     */
    virtual void __decRef();
    /** 
     * @brief 获取当前引用计数
     * 
     * @return 
     */
    virtual int __getRef() const;
    /** 
     * @brief 设置__noDelete标志
     * true: 当引用计数为0时不删除它管理的对象
     * false: 当引用计数为0时删除它管理的对象 
     * @param bool
     */
    virtual void __setNoDelete(bool);

protected:
    int _ref;
    bool _noDelete;
    Mutex _mutex;
};
}//end namespace
#endif
