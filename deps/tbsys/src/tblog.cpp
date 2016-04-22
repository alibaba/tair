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

#include "tblog.h"
#include "WarningBuffer.h"
#include <string.h>
#include <sys/uio.h>
namespace tbsys
{
LogExtraHeaderCallback LOG_EXTRA_HEADER_CB = NULL;
const char * const CLogger::_errstr[] = {"ERROR","USER_ERR","WARN","INFO","TRACE","DEBUG"};

CLogger::CLogger() {
    _fd = fileno(stderr);
    _wf_fd = fileno(stderr);
    _level = 9;
    _wf_level = 2; /* WARN */
    _name = NULL;
    _check = 0;
    _maxFileSize = 0;
    _maxFileIndex = 0;
    pthread_mutex_init(&_fileSizeMutex, NULL );
    pthread_mutex_init(&_fileIndexMutex, NULL );
    _flag = false;
    _wf_flag = false;
}

CLogger::~CLogger() {
    if (_name != NULL) {
        free(_name);
        _name = NULL;
        close(_fd);
        if (_wf_flag) close(_wf_fd);
    }
    pthread_mutex_destroy(&_fileSizeMutex);
    pthread_mutex_destroy(&_fileIndexMutex);
}

void CLogger::setLogLevel(const char *level, const char *wf_level)
{
    if (level == NULL) return;
    int l = sizeof(_errstr)/sizeof(char*);
    for (int i=0; i<l; i++) {
        if (strcasecmp(level, _errstr[i]) == 0) {
            _level = i;
            break;
        }
    }
    if (NULL != wf_level)
    {
      for (int j = 0; j < l; j++)
      {
        if (strcasecmp(wf_level, _errstr[j]) == 0)
        {
          _wf_level = j;
          break;
        }
      }
    }
}

void CLogger::setFileName(const char *filename, bool flag, bool open_wf)
{
    bool need_closing = false;
    if (_name) {
        need_closing = true;
        free(_name);
        _name = NULL;
    }
    _name = strdup(filename);
    int fd = open(_name, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE);
    _flag = flag;
    if (!_flag)
    {
      dup2(fd, _fd);
      dup2(fd, 1);
      if (_fd != 2) dup2(fd, 2);
      if (fd != _fd) close(fd);
    }
    else
    {
      if (need_closing)
      {
        close(_fd);
      }
      _fd = fd;
    }
    if (_wf_flag && need_closing)
    {
      close(_wf_fd);
    }
    //open wf file
    _wf_flag = open_wf;
    if (_wf_flag)
    {
      char tmp_file_name[256];
      memset(tmp_file_name, 0, sizeof(tmp_file_name));
      snprintf(tmp_file_name, sizeof(tmp_file_name), "%s.wf", _name);
      fd = open(tmp_file_name, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE);
      _wf_fd = fd;
    }
}

  static  char NEWLINE[1] = {'\n'};
  static const int MAX_LOG_SIZE = 64*1024; // 64kb
  void CLogger::logMessage(int level,const char *file, int line, const char *function, pthread_t tid, const char *fmt, ...) {
    if (level>_level) return;

    if (_check && _name) {
        checkFile();
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm;
    ::localtime_r((const time_t*)&tv.tv_sec, &tm);

    static __thread char data1[MAX_LOG_SIZE];
    char head[256];

    va_list args;
    va_start(args, fmt);
    int data_size = vsnprintf(data1, MAX_LOG_SIZE, fmt, args);
    va_end(args);
    if (data_size >= MAX_LOG_SIZE)
    {
      data_size = MAX_LOG_SIZE-1;
    }
    // remove trailing '\n'
    while (data1[data_size-1] == '\n') data_size --;
    data1[data_size] = '\0';

    int head_size;
    if (level < TBSYS_LOG_LEVEL_INFO) {
        head_size = snprintf(head,256,"[%04d-%02d-%02d %02d:%02d:%02d.%06ld] %-5s %s (%s:%d) [%ld] ",
                        tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
                        tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec,
                        _errstr[level], function, file, line, tid);
    } else {
        head_size = snprintf(head,256,"[%04d-%02d-%02d %02d:%02d:%02d.%06ld] %-5s %s:%d [%ld] ",
                        tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
                        tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec,
                        _errstr[level], file, line, tid);
    }
    //truncated
    if (head_size >= 256)
    {
      head_size = 255;
    }
    // extra header
    char extra_head[256];
    extra_head[0] = '\0';
    int32_t extra_head_size = 0;
    if (NULL != LOG_EXTRA_HEADER_CB)
    {
      extra_head_size = LOG_EXTRA_HEADER_CB(extra_head, 256,
                                            level, file, line, function, tid);
    }
    //truncated
    if (extra_head_size >= 256)
    {
      extra_head_size = 255;
    }
    struct iovec vec[4];
    vec[0].iov_base = head;
    vec[0].iov_len = head_size;
    vec[1].iov_base = extra_head;
    vec[1].iov_len = extra_head_size;
    vec[2].iov_base = data1;
    vec[2].iov_len = data_size;
    vec[3].iov_base = NEWLINE;
    vec[3].iov_len = sizeof(NEWLINE);
    if (data_size > 0)
    {
      ::writev(_fd, vec, 4);
      if (_wf_flag && level <= _wf_level)
        ::writev(_wf_fd, vec, 4);
    }
    if ( _maxFileSize ){
        pthread_mutex_lock(&_fileSizeMutex);
        off_t offset = ::lseek(_fd, 0, SEEK_END);
        if ( offset < 0 ){
            // we got an error , ignore for now
        } else {
            if ( static_cast<int64_t>(offset) >= _maxFileSize ) {
                rotateLog(NULL);
            }
        }
        pthread_mutex_unlock(&_fileSizeMutex);
    }

    // write data to warning buffer for SQL
    if (WarningBuffer::is_warn_log_on() && data_size > 0)
    {
      if (level == TBSYS_LOG_LEVEL_WARN)
      { // WARN only
        WarningBuffer *wb = get_tsi_warning_buffer();
        if (NULL != wb)
        {
          wb->append_warning(data1);
        }
      }
      else if (level == TBSYS_LOG_LEVEL_USER_ERROR)
      {
        WarningBuffer *wb = get_tsi_warning_buffer();
        if (NULL != wb)
        {
          wb->set_err_msg(data1);
        }
      }
    }
    return;
}

void CLogger::rotateLog(const char *filename, const char *fmt)
{
    if (filename == NULL && _name != NULL)
    {
        filename = _name;
    }
    char wf_filename[256];
    if (filename != NULL)
    {
      snprintf(wf_filename, sizeof(wf_filename), "%s.wf", filename);
    }
    if (access(filename, R_OK) == 0)
    {
        char oldLogFile[256];
        char old_wf_log_file[256];
        time_t t;
        time(&t);
        struct tm tm;
        localtime_r((const time_t*)&t, &tm);
        if (fmt != NULL)
        {
            char tmptime[256];
            strftime(tmptime, sizeof(tmptime), fmt, &tm);
            sprintf(oldLogFile, "%s.%s", filename, tmptime);
            snprintf(old_wf_log_file, sizeof(old_wf_log_file), "%s.%s", wf_filename, tmptime);
        }
        else
        {
            sprintf(oldLogFile, "%s.%04d%02d%02d%02d%02d%02d",
                filename, tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec);
            snprintf(old_wf_log_file, sizeof(old_wf_log_file), "%s.%04d%02d%02d%02d%02d%02d",
              wf_filename, tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
              tm.tm_hour, tm.tm_min, tm.tm_sec);
        }
        if ( _maxFileIndex > 0 )
        {
            pthread_mutex_lock(&_fileIndexMutex);
            if ( _fileList.size() >= _maxFileIndex )
            {
                std::string oldFile = _fileList.front();
                _fileList.pop_front();
                unlink( oldFile.c_str());
            }
            _fileList.push_back(oldLogFile);
            pthread_mutex_unlock(&_fileIndexMutex);
        }
        rename(filename, oldLogFile);
        if (_wf_flag && _maxFileIndex > 0)
        {
          pthread_mutex_lock(&_fileIndexMutex);
          if (_wf_file_list.size() >= _maxFileIndex)
          {
            std::string old_wf_file = _wf_file_list.front();
            _wf_file_list.pop_front();
            unlink(old_wf_file.c_str());
          }
          _wf_file_list.push_back(old_wf_log_file);
          pthread_mutex_unlock(&_fileIndexMutex);
        }
        rename(wf_filename, old_wf_log_file);
    }
    int fd = open(filename, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE);
    if (!_flag)
    {
      dup2(fd, _fd);
      dup2(fd, 1);
      if (_fd != 2) dup2(fd, 2);
      close(fd);
    }
    else
    {
      if (_fd != 2)
      {
        close(_fd);
      }
      _fd = fd;
    }
    if (_wf_flag)
    {
      fd = open(wf_filename, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE);
      if (_wf_fd != 2)
      {
        close(_wf_fd);
      }
      _wf_fd = fd;
    }
}

void CLogger::checkFile()
{
    struct stat stFile;
    struct stat stFd;

    fstat(_fd, &stFd);
    int err = stat(_name, &stFile);
    if ((err == -1 && errno == ENOENT)
        || (err == 0 && (stFile.st_dev != stFd.st_dev || stFile.st_ino != stFd.st_ino))) {
        int fd = open(_name, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE);
        if (!_flag)
        {
          dup2(fd, _fd);
          dup2(fd, 1);
          if (_fd != 2) dup2(fd, 2);
          close(fd);
        }
        else
        {
          if (_fd != 2)
          {
            close(_fd);
          }
          _fd = fd;
        }
    }
}

CLogger& CLogger::getLogger()
{
  static CLogger logger;
  return logger;
}

void CLogger::setMaxFileSize( int64_t maxFileSize)
{
                                           // 1GB
    if ( maxFileSize < 0x0 || maxFileSize > 0x40000000){
        maxFileSize = 0x40000000;//1GB
    }
    _maxFileSize = maxFileSize;
}

void CLogger::setMaxFileIndex( int maxFileIndex )
{
    if ( maxFileIndex < 0x00 ) {
        maxFileIndex = 0x0F;
    }
    if ( maxFileIndex > 0x400 ) {//1024
        maxFileIndex = 0x400;//1024
    }
    _maxFileIndex = maxFileIndex;
}
}

/////////////
