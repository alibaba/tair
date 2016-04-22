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

#ifndef TNET_TEST_TESTBASE_H
#define TNET_TEST_TESTBASE_H
#include <Shared.h>
#include <Handle.h>
#include <string>
#include <tbsys.h>

using namespace tbutil;

class testFailed
{
public:
    testFailed( const std::string&);
  
     const std::string _name;
};

class testBase : public Shared
{
public:
    testBase( const std::string&);
    std::string name() const;
    void start();
protected:
    virtual void run()=0;
    const std::string _name;    
};

typedef tbutil::Handle<testBase> testBasePtr;
#endif
