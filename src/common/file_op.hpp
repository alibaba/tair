/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef TAIR_COMMON_FILE_OP_H_
#define TAIR_COMMON_FILE_OP_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <fcntl.h>

namespace tair {
namespace common {
class FileOperation {
public:
    FileOperation(const std::string &file_name, const int open_flags = O_RDWR | O_LARGEFILE | O_CREAT);

    virtual ~FileOperation();

    int open_file();

    void close_file();

    virtual int flush_file();

    int flush_data();

    int unlink_file();

    int rename_file(const char *new_name);

    inline char *get_file_name() const {
        return file_name_;
    }

    virtual int64_t pread_file(char *buf, const int32_t nbytes, const int64_t offset);

    virtual int64_t pwrite_file(const char *buf, const int32_t nbytes, const int64_t offset);

    int write_file(const char *buf, const int32_t nbytes);

    int64_t get_file_size();

    int ftruncate_file(const int64_t length);

    int seek_file(const int64_t offset);

    int32_t current_pos();

    int get_fd() const {
        return fd_;
    }

protected:
    FileOperation();

    FileOperation(const FileOperation &);

    int check_file();

protected:
    static const int MAX_DISK_TIMES = 5;
    static const mode_t OPEN_MODE = 0644;

protected:
    int fd_;                // file handle
    int open_flags_;        // open flags
    char *file_name_;       // file path name
};
}
}

#endif //TAIR_COMMON_FILE_OP_H_
