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

#ifndef TBSYS_TIME_H
#define TBSYS_TIME_H
#include <sys/time.h>
#include "tbsys.h"
namespace tbutil
{
/** 
* @brief Time类提供对时间的简单操作,获取当前时间,构造时间间隔
* 时间加减，时间转换
*/
class Time
{
public:

    Time();

    enum Clock { Realtime, Monotonic };
    /** 
     * @brief 获取当前时间
     * 
     * @param clock Realtime: 系统rtc时间, Monotonic: 系统从boost起经过的时间
     * 
     * @return 
     */
    static Time now(Clock clock= Realtime);
    /** 
     * @brief 构造一个时间对象
     * 
     * @param usec: 构造的时间(秒)
     * 
     * @return 
     */
    static Time seconds(Int64 usec);
    /** 
     * @brief 构造一个时间对象 
     * 
     * @param milli : 构造的进间(毫秒)
     * 
     * @return 
     */
    static Time milliSeconds(Int64 milli);
    /** 
     * @brief 构造一个时间对象 
     * 
     * @param micro : 构造的时间(微秒)
     * 
     * @return 
     */
    static Time microSeconds(Int64 micro);
    
    /** 
     * @brief 将Time转换成timeval结构
     * 
     * @return 
     */
    operator timeval() const;

    /** 
     * @brief 将时间转换成秒
     * 
     * @return 
     */
    Int64 toSeconds() const;
    /** 
     * @brief 将时间转换成毫秒
     * 
     * @return 
     */
    Int64 toMilliSeconds() const;
    /** 
     * @brief 将时间转换成微秒
     * 
     * @return 
     */
    Int64 toMicroSeconds() const;

    double toSecondsDouble() const;
    double toMilliSecondsDouble() const;
    double toMicroSecondsDouble() const;

    /** 
     * @brief 将时间转换成字符串,例如: 2009-10-26 10:47:47.932
     * 
     * @return 
     */
    std::string toDateTime() const;
    /** 
     * @brief 将时间转换成字符串,例如: 14543d 02:47:47.932
     * 
     * @return 
     */
    std::string toDuration() const;

    Time operator-() const
    {
        return Time(-_usec);
    }

    Time operator-(const Time& rhs) const
    {
        return Time(_usec - rhs._usec);
    }

    Time operator+(const Time& rhs) const
    {
        return Time(_usec + rhs._usec);
    }

    Time& operator+=(const Time& rhs)
    {
        _usec += rhs._usec;
        return *this;
    }

    Time& operator-=(const Time& rhs)
    {
        _usec -= rhs._usec;
        return *this;
    }

    bool operator<(const Time& rhs) const
    {
        return _usec < rhs._usec;
    }

    bool operator<=(const Time& rhs) const
    {
        return _usec <= rhs._usec;
    }

    bool operator>(const Time& rhs) const
    {
        return _usec > rhs._usec;
    }

    bool operator>=(const Time& rhs) const
    {
        return _usec >= rhs._usec;
    }

    bool operator==(const Time& rhs) const
    {
        return _usec == rhs._usec;
    }

    bool operator!=(const Time& rhs) const
    {
        return _usec != rhs._usec;
    }

    double operator/(const Time& rhs) const
    {
        return (double)_usec / (double)rhs._usec;
    }

    Time& operator*=(int rhs)
    {
        _usec *= rhs;
        return *this;
    }

    Time operator*(int rhs) const
    {
        Time t;
        t._usec = _usec * rhs;
        return t;
    }

    Time& operator/=(int rhs)
    {
        _usec /= rhs;
        return *this;
    }

    Time operator/(int rhs) const
    {
        Time t;
        t._usec = _usec / rhs;
        return t;
    }

    Time& operator*=(Int64 rhs)
    {
        _usec *= rhs;
        return *this;
    }

    Time operator*(Int64 rhs) const
    {
        Time t;
        t._usec = _usec * rhs;
        return t;
    }

    Time& operator/=(Int64 rhs)
    {
        _usec /= rhs;
        return *this;
    }

    Time operator/(Int64 rhs) const
    {
        Time t;
        t._usec = _usec / rhs;
        return t;
    }

    Time& operator*=(double rhs)
    {
        _usec = static_cast<Int64>(static_cast<double>(_usec) * rhs);
        return *this;
    }

    Time operator*(double rhs) const
    {
        Time t;
        t._usec = static_cast<Int64>(static_cast<double>(_usec) * rhs);
        return t;
    }

    Time& operator/=(double rhs)
    {
        _usec = static_cast<Int64>(static_cast<double>(_usec) / rhs);
        return *this;
    }

    Time operator/(double rhs) const
    {
        Time t;
        t._usec = static_cast<Int64>(static_cast<double>(_usec) / rhs);
        return t;
    }

    Time(Int64);

private:

    Int64 _usec;
};
}//end namespace 
#endif
