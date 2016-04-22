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

#include <iomanip>
#include <sys/time.h>
#include "Time.h"
#include "Exception.h"

namespace tbutil
{
Time::Time() :
    _usec(0)
{
}

Time Time::now(Clock clock)
{
    if(clock == Realtime)
    {
        struct timeval tv;
        if(gettimeofday(&tv, 0) < 0)
        {
#ifdef _NO_EXCEPTION
            TBSYS_LOG(ERROR,"%s","SyscallException");
            assert( 0 );
#else
            throw SyscallException(__FILE__, __LINE__, errno);
#endif
        }
        return Time(tv.tv_sec * T_INT64(1000000) + tv.tv_usec);
    }
    else // Monotonic
    {
        struct timespec ts;
        if(clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
        {
#ifdef _NO_EXCEPTION
            TBSYS_LOG(ERROR,"%s","SyscallException");
            assert(0);
#else
            throw SyscallException(__FILE__, __LINE__, errno);
#endif
        }
        return Time(ts.tv_sec * T_INT64(1000000) + ts.tv_nsec / T_INT64(1000));
    }
}

Time Time::seconds(Int64 t)
{
    return Time(t * T_INT64(1000000));
}

Time Time::milliSeconds(Int64 t)
{
    return Time(t * T_INT64(1000));
}

Time Time::microSeconds(Int64 t)
{
    return Time(t);
}

Time::operator timeval() const
{
    timeval tv;
    tv.tv_sec = static_cast<long>(_usec / 1000000);
    tv.tv_usec = static_cast<long>(_usec % 1000000);
    return tv;
}

Int64 Time::toSeconds() const
{
    return _usec / 1000000;
}

Int64 Time::toMilliSeconds() const
{
    return _usec / 1000;
}

Int64 Time::toMicroSeconds() const
{
    return _usec;
}

double Time::toSecondsDouble() const
{
    return _usec / 1000000.0;
}

double Time::toMilliSecondsDouble() const
{
    return _usec / 1000.0;
}

double Time::toMicroSecondsDouble() const
{
    return static_cast<double>(_usec);
}

std::string Time::toDateTime() const
{
    time_t time = static_cast<long>(_usec / 1000000);

    struct tm* t;
    struct tm tr;
    localtime_r(&time, &tr);
    t = &tr;

    char buf[32];
    strftime(buf, sizeof(buf), "%F %H:%M:%S", t);
    //strftime(buf, sizeof(buf), "%x %H:%M:%S", t);

    std::ostringstream os;
    os << buf << ".";
    os.fill('0');
    os.width(3);
    os << static_cast<long>(_usec % 1000000 / 1000);
    return os.str();
}

std::string Time::toDuration() const
{
    Int64 usecs = _usec % 1000000;
    Int64 secs = _usec / 1000000 % 60;
    Int64 mins = _usec / 1000000 / 60 % 60;
    Int64 hours = _usec / 1000000 / 60 / 60 % 24;
    Int64 days = _usec / 1000000 / 60 / 60 / 24;

    using namespace std;

    ostringstream os;
    if(days != 0)
    {
        os << days << "d ";
    }
    os << setfill('0') << setw(2) << hours << ":" << setw(2) << mins << ":" << setw(2) << secs;
    if(usecs != 0)
    {
        os << "." << setw(3) << (usecs / 1000);
    }

    return os.str();
}

Time::Time(Int64 usec) :
    _usec(usec)
{
}

}//end namespace tbutil
