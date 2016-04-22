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

#include <ostream>
#include <cstdlib>
#include "Exception.h"
#include "StaticMutex.h"
using namespace std;
namespace tbutil
{
Exception::Exception() :
    _file(0),
    _line(0)
{
}
    
Exception::Exception(const char* file, int line) :
    _file(file),
    _line(line)
{
}
    
Exception::~Exception() throw()
{
}

const char* Exception::_name = "tbutil::Exception";

string Exception::name() const
{
    return _name;
}

void Exception::print(ostream& out) const
{
    if(_file && _line > 0)
    {
        out << _file << ':' << _line << ": ";
    }
    out << name();
}

const char* Exception::what() const throw()
{
    try
    {
        StaticMutex::Lock lock(globalMutex);
        {
            if(_str.empty())
            {
                stringstream s;
                print(s);
                _str = s.str(); // Lazy initialization.
            }
        }
        return _str.c_str();
    }
    catch(...)
    {
    }
    return "";
}

Exception* Exception::clone() const
{
    return new Exception(*this);
}

void Exception::_throw() const
{
    throw *this;
}

const char* Exception::file() const
{
    return _file;
}

int Exception::line() const
{
    return _line;
}

std::ostream& operator << (std::ostream& out, const Exception& ex)
{
    ex.print(out);
    return out;
}

NullHandleException::NullHandleException(const char* file, int line) :
    Exception(file, line)
{
    //if(nullHandleAbort)
    {
        abort();
    }
}

NullHandleException::~NullHandleException() throw()
{
}

const char* NullHandleException::_name = "NullHandleException";

string NullHandleException::name() const
{
    return _name;
}

Exception* NullHandleException::clone() const
{
    return new NullHandleException(*this);
}

void NullHandleException::_throw() const
{
    throw *this;
}

IllegalArgumentException::IllegalArgumentException(const char* file, int line) :
    Exception(file, line)
{
}

IllegalArgumentException::IllegalArgumentException(const char* file, int line, const string& r) :
    Exception(file, line),
    _reason(r)
{
}

IllegalArgumentException::~IllegalArgumentException() throw()
{
}

const char* IllegalArgumentException::_name = "IllegalArgumentException";

string IllegalArgumentException::name() const
{
    return _name;
}

void IllegalArgumentException::print(ostream& out) const
{
    Exception::print(out);
    out << ": " << _reason;
}

Exception* IllegalArgumentException::clone() const
{
    return new IllegalArgumentException(*this);
}

void IllegalArgumentException::_throw() const
{
    throw *this;
}

string IllegalArgumentException::reason() const
{
    return _reason;
}

SyscallException::SyscallException(const char* file, int line):
    Exception(file,line)
{

}
 
SyscallException::SyscallException(const char* file, int line, int err ): 
    Exception(file, line),
    _error(err)
{
}
    
const char* SyscallException::_name = "SyscallException";

string SyscallException::name() const
{
    return _name;
}

void SyscallException::print(ostream& os) const
{
    Exception::print(os);
    if(_error != 0)
    {
    }
}

Exception* SyscallException::clone() const
{
    return new SyscallException(*this);
}

void SyscallException::_throw() const
{
    throw *this;
}

int SyscallException::error() 
{
    return _error;
}
}//end namespace tbutil

