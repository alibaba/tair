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

#include "testBase.h"
#include <Exception.h>
using namespace std;
using namespace tbutil;


testFailed::testFailed(const std::string& name ):
   _name( name)
{

}

testBase::testBase(const string& name):
   _name( name )
{

}

std::string testBase::name() const
{
    return _name;
}

void testBase::start()
{
    cout<<"running "<<_name<<endl;
    try
    {
        run();
    }
    catch( const Exception& e )
    {
       cout<<e <<" failed" <<endl;
       throw testFailed(_name);
    } 
    cout <<" ok "<<endl;
}
