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

#ifndef TBSYS_CTRL_C_HANDLER_H
#define TBSYS_CTRL_C_HANDLER_H

#include "Exception.h"

namespace tbutil
{
typedef void (*CtrlCHandlerCallback)(int);

/** 
 * @brief CtrlCHanler 用于处理Ctrl+C 及其他类似的发给C++ 进程的信号
 * 在任一时刻，在一个进程中只能有 一个CtrlCHandler 实例
 */
class CtrlCHandler
{
public:

/** 
* @brief 用一个回调函数构造一个实例
* 
* @param CtrlCHandlerCallback
*/
    CtrlCHandler(CtrlCHandlerCallback = 0);
    ~CtrlCHandler();

    /** 
     * @brief 设置新的回调函数
     * 
     * @param callback 
     */
    void setCallback(CtrlCHandlerCallback callback);
    /** 
     * @brief 获得当前回调函数
     * 
     * @return 
     */
    CtrlCHandlerCallback getCallback() const;
};


/** 
 * @brief CtrlCHandler实例如果已经存在就会抛出异常
 */
class CtrlCHandlerException : public Exception
{ 
public:
 
    CtrlCHandlerException(const char*, int);
    virtual std::string _name() const;
    virtual Exception* _clone() const;
    virtual void _throw() const;
};
}//end namespace
#endif
