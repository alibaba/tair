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

#include <tblog.h>
#include <unistd.h>

using namespace tbsys;

int main(int argc, char *argv[])
{
    TBSYS_LOG(INFO, "xxx: %s:%d", "xxxx", 1);
    TBSYS_LOG(ERROR, "xxx: %s:%d", "xxxx", 1);

    TBSYS_LOGGER.setFileName("/tmp/test.txt");

    for(int i=0; i<50; i++) {
        TBSYS_LOG(ERROR, "xxx: %s:%d", "xxxx", i);
        TBSYS_LOG(WARN, "xxx: %s:%d", "xxxx", i);
        TBSYS_LOG(INFO, "xxx: %s:%d", "xxxx", i);
        TBSYS_LOG(DEBUG, "xxx: %s:%d", "xxxx", i);
        //getchar();
    }
    //test rotateLog()
    CLogger logger;
    logger.setFileName("/tmp/test.log", false, true);
    logger.setLogLevel("INFO");
    logger.setMaxFileIndex(100);
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 50; j++)
      {
        logger.logMessage(TBSYS_LOG_LEVEL_ERROR, __FILE__, __LINE__, __FUNCTION__,
          pthread_self(), "test rotateLog(): %d", j);
      }
      logger.rotateLog(NULL);
      sleep(2);
    }

    return 0;
}

