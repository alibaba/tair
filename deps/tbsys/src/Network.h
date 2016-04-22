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

#ifndef TBSYS_NETWORK_H
#define TBSYS_NETWORK_H

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

#define SOCKET int
#define SOCKET_ERROR -1
#define INVALID_SOCKET -1

#ifndef SHUT_RD
    #define SHUT_RD 0
#endif

#ifndef SHUT_WR
    #define SHUT_WR 1
#endif

#ifndef SHUT_RDWR
    #define SHUT_RDWR 2
#endif

namespace tbutilInternal
{

bool interrupted();

int setBlock( SOCKET fd , bool block );

int createPipe(SOCKET fds[2]);

int closeSocketNoThrow( SOCKET fd );
}
#endif
