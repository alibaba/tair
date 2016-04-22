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

#ifndef TBSYS_THREADEXCEPTION_H
#define TBSYS_THREADEXCEPTION_H
#include "Exception.h"
#include "Time.h"

namespace tbutil
{
/** 
* @brief 调用线程相关的系统调用异常类 
*/
class ThreadSyscallException : public SyscallException
{
public:

    ThreadSyscallException(const char*, int, int);
    virtual std::string name() const;
    virtual Exception* clone() const;
    virtual void _throw() const;

private:

    static const char* _name;
};

/** 
 * @brief 当前线程已经被锁住异常
 */
class ThreadLockedException : public Exception
{
public:

    ThreadLockedException(const char*, int);
    virtual std::string name() const;
    virtual Exception* clone() const;
    virtual void _throw() const;

private:

    static const char* _name;
};

/** 
 * @brief 线程Start时的异常
 */
class ThreadStartedException : public Exception
{
public:

    ThreadStartedException(const char*, int);
    virtual std::string name() const;
    virtual Exception* clone() const;
    virtual void _throw() const;

private:

    static const char* _name;
};

/** 
 * @brief 线程没有调用Start异常
 */
class ThreadNotStartedException : public Exception
{
public:

    ThreadNotStartedException(const char*, int);
    virtual std::string name() const;
    virtual Exception* clone() const;
    virtual void _throw() const;

private:

    static const char* _name;
};

/** 
 * @brief 调用线程控制系统调用异常
 */
class BadThreadControlException : public Exception
{
public:

    BadThreadControlException(const char*, int);
    virtual std::string name() const;
    virtual Exception* clone() const;
    virtual void _throw() const;

private:

    static const char* _name;
};

/** 
 * @brief 超时异常 
 */
class InvalidTimeoutException : public Exception
{
public:

    InvalidTimeoutException(const char*, int, const Time&);
    virtual std::string name() const;
    virtual void print(std::ostream&) const;
    virtual Exception* clone() const;
    virtual void _throw() const;

private:
    
    Time _timeout;
    static const char* _name;
};

/** 
 * @brief 创建线程异常
 */
class ThreadCreateException: public Exception
{
public:
      ThreadCreateException( const char* , int );
      virtual std::string name() const;
      virtual void print(std::ostream&) const;
      virtual Exception* clone() const;
      virtual void _throw() const;
private: 
      static const char* _name;
};
}//end namespace
#endif

