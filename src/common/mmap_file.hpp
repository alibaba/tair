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

#ifndef TAIR_MMAP_FILE_H
#define TAIR_MMAP_FILE_H

#include <unistd.h>
#include <sys/mman.h>
#include "log.hpp"

namespace tair {

class file_mapper {
public:
    file_mapper() {
        data = NULL;
        size = 0;
        fd = -1;
    }

    ~file_mapper() {
        close_file();
    }

    void close_file() {
        if (data) {
            munmap(data, size);
            close(fd);
            data = NULL;
            size = 0;
            fd = -1;
        }
    }


    void sync_file() {
        if (data != NULL && size > 0) {
            msync(data, size, MS_ASYNC);
        }
    }

    /**
     * open file
     *
     * createLength == 0 means read only
     */
    bool open_file(const char *file_name, int create_length = 0) {
        int flags = PROT_READ;
        if (create_length > 0) {
            fd = open(file_name, O_RDWR | O_LARGEFILE | O_CREAT, 0644);
            flags = PROT_READ | PROT_WRITE;
        } else {
            fd = open(file_name, O_RDONLY | O_LARGEFILE);
        }

        if (fd < 0) {
            log_error("open file : %s failed, errno: %d", file_name, errno);
            return false;
        }

        if (create_length > 0) {
            if (ftruncate(fd, create_length) != 0) {
                log_error("ftruncate file: %s failed", file_name);
                close(fd);
                fd = -1;
                return false;
            }
            size = create_length;
        } else {
            struct stat stbuff;
            fstat(fd, &stbuff);
            size = stbuff.st_size;
        }

        data = mmap(0, size, flags, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            log_error("map file: %s failed ,err is %s(%d)", file_name, strerror(errno), errno);
            close(fd);
            fd = -1;
            data = NULL;
            return false;
        }
        return true;
    }

    void *get_data() const {
        return data;
    }

    int get_size() const {
        return size;
    }

    int get_modify_time() const {
        struct stat buffer;
        if (fd >= 0 && fstat(fd, &buffer) == 0) {
            return (int) buffer.st_mtime;
        }
        return 0;
    }

private:
    file_mapper(const file_mapper &);

    file_mapper &operator=(const file_mapper &);

    void *data;
    int size;
    int fd;
};

const static int MB_SIZE = (1 << 20);

class mmap_file {

public:

    mmap_file() {
        data = NULL;
        max_size = 0;
        size = 0;
        fd = -1;
    }

    mmap_file(int size, int fd) {
        max_size = size;
        this->fd = fd;
        data = NULL;
        this->size = 0;
    }

    ~mmap_file() {
        if (data) {
            msync(data, size, MS_SYNC);        // make sure synced
            munmap(data, size);
            log_debug("mmap unmapped, size is: [%lu]", size);
            data = NULL;
            size = 0;
            fd = -1;
        }
    }

    bool sync_file() {
        if (data != NULL && size > 0) {
            return msync(data, size, MS_ASYNC) == 0;
        }
        return true;
    }

    bool map_file(bool write = false) {
        int flags = PROT_READ;

        if (write)
            flags |= PROT_WRITE;

        if (fd < 0)
            return false;

        if (0 == max_size)
            return false;

        if (max_size <= (1024 * MB_SIZE)) {
            size = max_size;
        } else {
            size = 1 * MB_SIZE;
        }

        if (!ensure_file_size(size)) {
            log_error("ensure file size failed");
            return false;
        }

        data = mmap(0, size, flags, MAP_SHARED, fd, 0);

        if (data == MAP_FAILED) {
            log_error("map file failed: %s", strerror(errno));
            fd = -1;
            data = NULL;
            size = 0;
            return false;
        }

        log_info("mmap file successed, maped size is: [%lu]", size);

        return true;
    }

    bool remap() {
        if (fd < 0 || data == NULL) {
            log_error("mremap not mapped yet");
            return false;
        }

        if (size == max_size) {
            log_info("already mapped max size, currSize: [%lu], maxSize: [%lu]",
                     size, max_size);
            return false;
        }

        size_t new_size = size * 2;
        if (new_size > max_size)
            new_size = max_size;

        if (!ensure_file_size(new_size)) {
            log_error("ensure file size failed in mremap");
            return false;
        }

        //void *newMapData = mremap(m_data, m_size, newSize, MREMAP_MAYMOVE);
        void *new_map_data = mremap(data, size, new_size, 0);
        if (new_map_data == MAP_FAILED) {
            log_error("mremap file failed: %s", strerror(errno));
            return false;
        } else {
            log_info("remap success, oldSize: [%lu], newSize: [%lu]", size,
                     new_size);
        }

        log_info("mremap successed, new size: [%lu]", new_size);
        data = new_map_data;
        size = new_size;
        return true;
    }


    void *get_data() {
        return data;
    }

    size_t get_size() {
        return size;
    }

private:
    bool ensure_file_size(int size) {
        struct stat s;
        if (fstat(fd, &s) < 0) {
            log_error("fstat error, {%s}", strerror(errno));
            return false;
        }
        if (s.st_size < size) {
            if (ftruncate(fd, size) < 0) {
                log_error("ftruncate file to size: [%u] failed. {%s}", size,
                          strerror(errno));
                return false;
            }
        }

        return true;
    }

private:
    size_t max_size;
    size_t size;
    int fd;
    void *data;
};
}

#endif
