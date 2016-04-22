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

#ifndef TBSYS_LOG_H
#define TBSYS_LOG_H

#include <stdarg.h>
#include <time.h>
#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <deque>
#include <string>
#include <pthread.h>
#include <sys/time.h>

#define TBSYS_LOG_LEVEL_ERROR 0
#define TBSYS_LOG_LEVEL_USER_ERROR  1
#define TBSYS_LOG_LEVEL_WARN  2
#define TBSYS_LOG_LEVEL_INFO  3
#define TBSYS_LOG_LEVEL_TRACE 4
#define TBSYS_LOG_LEVEL_DEBUG 5
#define TBSYS_LOG_LEVEL(level) TBSYS_LOG_LEVEL_##level, __FILE__, __LINE__, __FUNCTION__, pthread_self()
#define TBSYS_LOG_NUM_LEVEL(level) level, __FILE__, __LINE__, __FUNCTION__, pthread_self()
#define TBSYS_LOGGER tbsys::CLogger::getLogger()
#define TBSYS_PRINT(level, ...) TBSYS_LOGGER.logMessage(TBSYS_LOG_LEVEL(level), __VA_ARGS__)
#define TBSYS_LOG_BASE(level, ...) (TBSYS_LOG_LEVEL_##level>TBSYS_LOGGER._level) ? (void)0 : TBSYS_PRINT(level, __VA_ARGS__)
#define TBSYS_LOG(level, _fmt_, args...) ((TBSYS_LOG_LEVEL_##level>TBSYS_LOGGER._level) ? (void)0 : TBSYS_LOG_BASE(level,_fmt_, ##args))
#define TBSYS_LOG_US(level, _fmt_, args...) \
  ((TBSYS_LOG_LEVEL_##level>TBSYS_LOGGER._level) ? (void)0 : TBSYS_LOG_BASE(level, "[%ld][%ld][%ld] " _fmt_, \
                                                            pthread_self(), tbsys::CLogger::get_cur_tv().tv_sec, \
                                                            tbsys::CLogger::get_cur_tv().tv_usec, ##args))

typedef int32_t (*LogExtraHeaderCallback)(char *buf, int32_t buf_size,
                                          int level, const char *file, int line, const char *function, pthread_t tid);

namespace tbsys {
using std::deque;
using std::string;

extern LogExtraHeaderCallback LOG_EXTRA_HEADER_CB;
/**
* @brief ¼òµ¥µÄÈÕÖ¾ÏµÍ³
*/
class           CLogger {
public:

  static const mode_t LOG_FILE_MODE = 0644;
    CLogger();
    ~CLogger();
    /**
     * @brief
     *
     * @param filename
     * @param fmt
     */
    void rotateLog(const char *filename, const char *fmt = NULL);
    /**
     * @brief ½«ÈÕÖ¾ÄÚÈÝÐ´ÈëÎÄ¼þ
     *
     * @param level ÈÕÖ¾µÄ¼¶±ð
     * @param file  ÈÕÖ¾ÄÚÈÝËùÔÚµÄÎÄ¼þ
     * @param line  ÈÕÖ¾ÄÚÈÝËùÔÚµÄÎÄ¼þµÄÐÐºÅ
     * @param function Ð´ÈëÈÕÖ¾ÄÚÈÝµÄº¯ÊýÃû³Æ
     * @param fmt
     * @param ...
     */
    void logMessage(int level, const char *file, int line, const char *function, pthread_t tid, const char *fmt, ...) __attribute__ ((format (printf, 7, 8)));
    /**
     * @brief set log putout level
     *
     * @param level DEBUG|WARN|INFO|TRACE|ERROR
     *
     * @param wf_level set the level putout to wf log file
     */
    void setLogLevel(const char *level, const char *wf_level = NULL);
    /**
     * @brief set log file name
     *
     * @param filename log file name
     *
     * @param flag whether to redirect stdout to log file, if false, redirect it
     *
     * @param open_wf whether to open wf log file, default close
     */
    void setFileName(const char *filename, bool flag = false, bool open_wf = false);
    /**
     * @brief ¼ì²âÎÄ¼þÊÇ·ñÒÑ¾­´ò¿ª,±ê×¼Êä³ö,´íÎóÊä³öÖØ¶¨Ïò
     */
    void checkFile();
    void setCheck(int v) {_check = v;}
    /**
     * @brief ÉèÖÃÈÕÖ¾ÎÄ¼þÎÄ¼þµÄ´óÐ¡,´ïµ½maxFileSize¾ÍÐÂ´ò¿ªÒ»¸öÎÄ¼þ
     * Èç¹û²»ÉèÖÃ´ËÏî£¬ÈÕÖ¾ÏµÍ³»áºöÂÔÈÕÖ¾¹ö¶¯
     *
     * @param maxFileSize ÈÕÖ¾ÎÄ¼þµÄ´óÐ¡
     */
    void setMaxFileSize( int64_t maxFileSize=0x40000000);
    /**
     * @brief ±£Áô×î½ümaxFileIndex¸öÈÕÖ¾ÎÄ¼þ£¬³¬³ömaxFileIndex¸öÈÕÖ¾ÎÄ¼þ
     * »á°´Ê±¼äÏÈºóÉ¾³ý,µ«½ø³ÌÖØÆôºóÈÕÖ¾ÏµÍ³»á°´Ê±¼äÏÈºóÖØÐÂÍ³¼Æ
     *
     * @param maxFileIndex ±£ÁôÎÄ¼þµÄ×î´ó¸öÊý
     */
    void setMaxFileIndex( int maxFileIndex= 0x0F);

    static inline struct timeval get_cur_tv()
    {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      return tv;
    };

    static CLogger& getLogger();

private:
    int _fd;
    int _wf_fd;
    char *_name;
    int _check;
    size_t _maxFileIndex;
    int64_t _maxFileSize;
    bool _flag;
    bool _wf_flag;

public:
    int _level;
    int _wf_level;

private:
    std::deque<std::string> _fileList;
    std::deque<std::string> _wf_file_list;
    static const char *const _errstr[];
    pthread_mutex_t _fileSizeMutex;
    pthread_mutex_t _fileIndexMutex;
};

}
#endif
