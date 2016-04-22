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

#include "tbsys.h"
#include "Network.h"
#include "Exception.h"
namespace tbutilInternal
{

bool interrupted()
{
    return errno == EINTR;
}

int setBlock( SOCKET fd , bool block )
{
    if( block )
    {
        int flags = fcntl(fd,F_GETFL);
        flags &=~O_NONBLOCK;
        if(fcntl(fd,F_SETFL,flags) == SOCKET_ERROR )
        {
        #ifdef _NO_EXCEPTION
            closeSocketNoThrow( fd );
            return errno;
        #else
            closeSocketNoThrow( fd );
            //SocketException ex(__FILE__,__LINE__);
            //ex.error = errno;
            //throw ex;
        #endif
        }
    }
    else
    {
        int flags = fcntl(fd,F_GETFL);
        flags |=O_NONBLOCK;
        if(fcntl(fd,F_SETFL,flags) == SOCKET_ERROR )
        {
        #ifdef _NO_EXCEPTION
            closeSocketNoThrow( fd );
            return errno;
        #else
            closeSocketNoThrow( fd );
            /*SocketException ex(__FILE__,__LINE__);
            ex.error = errno;
            throw ex;*/
        #endif
        }       
    }
    return EXIT_SUCCESS;
}

int createPipe(SOCKET fds[2])
{
    if ( ::pipe(fds) != 0 )
    {
    #ifdef _NO_EXCEPTION
        return -1;
    #else
        tbutil::SyscallException ex(__FILE__,__LINE__);
        ex._error = tbutil::getSystemErrno();
        throw ex;
    #endif
    }
#ifdef _NO_EXCEPTION
    const int iRet = setBlock(fds[0],true);
    if ( iRet != 0 )
    {
        return iRet;
    }
    const int iRet2 = setBlock(fds[1] , true );
    if ( iRet2 != 0 )
    {
        return iRet2;
    }
#else
    try
    {
        setBlock(fds[0] , true );
    }
    catch(...)
    {
        closeSocketNoThrow( fds[0] );
        throw;
    }

    try
    {
        setBlock(fds[1] , true );
    }
    catch(...)
    {
        closeSocketNoThrow( fds[1] );
        throw;
    }
#endif
    return EXIT_SUCCESS;
} 

int closeSocketNoThrow( SOCKET fd )
{
    const int error = errno;
    close( fd );
    errno = error;
    return errno;
}
}
