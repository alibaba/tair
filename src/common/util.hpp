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

#ifndef TAIR_MHASH_HPP
#define TAIR_MHASH_HPP

#include <sys/time.h>
#include <sys/types.h>
#include <dirent.h>
#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <list>
#include "hash.hpp"
#include "define.hpp"
#include "log.hpp"

#include "tair_atomic.hpp"

#include <malloc.h>

#ifdef WITH_TCMALLOC
#  include <google/malloc_extension.h>
#endif

namespace tair {
namespace util {
class directory {
public:
    static std::string current() {
        char buffer[256];
        if (getcwd(buffer, 255) < 0) {
            return "";
        }
        return std::string(buffer);
    }
};

class hash_util {
public:
    static unsigned int mhash1(const char *key, int size) {
        return mur_mur_hash2(key, size, 134217689);
    }

    static unsigned int mhash2(const char *key, int size) {
        return mur_mur_hash2(key, size, 97);
    }

};

class string_util {
public:

    static char *conv_show_string(char *str, int size, char *ret = NULL, int msize = 0) {
        int index = 0;
        if (ret == NULL) {
            msize = size * 3 + 5;
            ret = (char *) malloc(msize);
        }
        unsigned char *p = (unsigned char *) str;
        while (size-- > 0 && index < msize - 4) {
            index += sprintf(ret + index, "\\%02X", *p);
            p++;
        }
        ret[index] = '\0';
        return ret;
    }

    static char *bin2ascii(char *str, int size, char *ret = NULL, int msize = 0) {
        if (ret == NULL) {
            msize = size * 3 + 5;
            ret = (char *) malloc(msize);
        }
        unsigned char *p = (unsigned char *) str;
        int i = 0;
        while (size-- > 0 && i < msize - 4) {
            if (*p >= '!' && *p <= '~') { //~ printable, excluding space
                i += sprintf(ret + i, "%c", *p++);
            } else {
                i += sprintf(ret + i, "\\%02X", *p++);
            }
        }
        ret[i] = '\0';
        return ret;
    }

    static void conv_raw_string(const char *str, char *result, int *size) {
        int index = 0;
        const unsigned char *p = (const unsigned char *) str;
        while (*p && index < (*size) - 1) {
            if (*p == '\\') {
                int c1 = *(p + 1);
                int c2 = *(p + 2);
                if (c1 == 0 || c2 == 0) break;
                if (isupper(c1)) c1 = tolower(c1);
                int value = (c1 >= '0' && c1 <= '9' ? c1 - '0' : c1 - 'a' + 10) * 16;
                if (isupper(c2)) c2 = tolower(c2);
                value += c2 >= '0' && c2 <= '9' ? c2 - '0' : c2 - 'a' + 10;
                result[index++] = (char) (value & 0xff);
                p += 2;
            } else {
                result[index++] = *p;
            }
            p++;
        }
        *size = index;
    }

    static unsigned int mur_mur_hash(const void *key, int len) {
        return mur_mur_hash2(key, len, 97);
    }

    static void split_str(const char *str, const char *delim, std::vector<std::string> &values) {
        if (NULL == str) {
            return;
        }
        if (NULL == delim) {
            values.push_back(str);
        } else {
            const char *pos = str + strspn(str, delim);
            size_t len = 0;
            while ((len = strcspn(pos, delim)) > 0) {
                values.push_back(std::string(pos, len));
                pos += len;
                pos += strspn(pos, delim);
            }
        }
    }

    static std::string trim_str(const std::string &src, const char *delim) {
        size_t begin = 0;
        size_t end = 0;
        std::string substr;

        size_t pos = src.find_first_not_of(delim);
        if (pos == std::string::npos) {
            begin = src.size();
        } else {
            begin = pos;
        }

        pos = src.find_last_not_of(delim);
        if (pos == std::string::npos) {
            end = src.size();
        } else {
            end = pos + 1;
        }
        if (begin < end) {
            substr = src.substr(begin, end - begin);
        }
        return substr;
    }

    /*
     * caller should ensure that `s' consists of digits
     */
    static uint64_t strntoul(const char *s, size_t n) {
        uint64_t ret = 0;
        for (size_t i = 0; i < n; ++i) {
            ret = ret * 10 + (s[i] - '0');
        }
        return ret;
    }
};

class time_util {
public:
    static int get_time_range(const char *str, int32_t &min, int32_t &max) {
        bool ret = str != NULL;

        if (ret) {
            int32_t tmp_min = 0, tmp_max = 0;
            char buf[32];
            char *max_p = strncpy(buf, str, sizeof(buf));
            char *min_p = strsep(&max_p, "-~");

            if (min_p != NULL && min_p[0] != '\0') {
                tmp_min = atoi(min_p);
            }
            if (max_p != NULL && max_p[0] != '\0') {
                tmp_max = atoi(max_p);
            }

            if ((ret = tmp_min >= 0 && tmp_max >= 0)) {
                min = tmp_min;
                max = tmp_max;
            }
        }

        return ret;
    }

    static bool is_in_range(int32_t min, int32_t max) {
        time_t t = time(NULL);
        struct tm *tm = localtime((const time_t *) &t);
        bool reverse = false;
        if (min > max) {
            std::swap(min, max);
            reverse = true;
        }
        bool in_range = tm->tm_hour >= min && tm->tm_hour <= max;
        return reverse ? !in_range : in_range;
    }

    static std::string time_to_str(time_t t) {
        struct tm r;
        char buf[32];
        memset(&r, 0, sizeof(r));
        if (localtime_r((const time_t *) &t, &r) == NULL) {
            fprintf(stderr, "TIME: %s (%d)\n", strerror(errno), errno);
            return "";
        }
        snprintf(buf, sizeof(buf), "%04d%02d%02d%02d%02d%02d",
                 r.tm_year + 1900, r.tm_mon + 1, r.tm_mday,
                 r.tm_hour, r.tm_min, r.tm_sec);
        return std::string(buf);
    }

    inline static int s2time(const char *s, time_t *ot) {
        int year, mouth, day, hour, min, sec;

        sscanf(s, "%04d%02d%02d%02d%02d%02d",
               &year, &mouth, &day, &hour, &min, &sec);

        // FIXME about 2014120229121212
        if (year < 1900 || mouth > 12 || mouth < 1 || day < 1 || day > 31 || hour > 23 || hour < 0 ||
            min > 59 || min < 0 || sec > 59 || sec < 0) {
            return 0;
        }
        struct tm t;
        t.tm_year = year - 1900;
        t.tm_mon = mouth - 1;
        t.tm_mday = day;
        t.tm_hour = hour;
        t.tm_min = min;
        t.tm_sec = sec;

        *ot = mktime(&t);
        return 1;
    }

    inline static int str_to_time(const std::string str, time_t *t) {
        return s2time(str.c_str(), t);
    }
};

class local_server_ip {
public:
    static uint64_t ip;
};

class file_util {
public:
    enum FileUtilChangeType {
        CHANGE_FILE_MODIFY = 0,
        CHANGE_FILE_ADD,
        CHANGE_FILE_DELETE,
    };

    enum OpResult {
        RETURN_SUCCESS,
        RETURN_KEY_VALUE_NUM_NOT_EQUAL,
        RETURN_OPEN_FILE_FAILED,
        RETURN_SECTION_NAME_NOT_FOUND,
        RETURN_KEY_VALUE_NOT_FOUND,
        RETURN_KEY_VALUE_ALREADY_EXIST,
        RETURN_SAVE_FILE_FAILD,
        RETURN_INVAL_OP_TYPE
    };
private:
    /*
     * @brief  for line in lines[line_begin, line_end), if line is regular match "[ \t]*$key[ \t]$kv_delimiter[ \t]*$value[ \t]*\n", then line.clear().
     *          lines.size() doesn't change.  every key has a coressponding result, after invoke, results.size() == keys.size().
     */
    static void del_kv_from_lines(const std::vector<std::string> &keys, const std::vector<std::string> &values,
                                  char kv_delimiter, size_t line_begin, size_t line_end,
                                  std::vector<std::string> &lines, std::vector<file_util::OpResult> &results);

    /*
     * @brief  if key value, doesn't exist in lines, append to lines[x], lines[x] is last line which has the same key, if none, append to the last line.
     *         of course, '\n' is append between them. every key has a coressponding result, after invoke, results.size() == keys.size().
     *         lines.size() doesn't change.
     *         if a line is regular match "[ \t]*$key[ \t]$kv_delimiter[ \t]*$value.*", we think this kv already existed in lines, and don't append it to lines.
     */
    static void add_kv_to_lines(const std::vector<std::string> &keys, const std::vector<std::string> &values,
                                char kv_delimiter, size_t line_begin, size_t line_end,
                                std::vector<std::string> &lines, std::vector<file_util::OpResult> &results);

public:
    static int change_conf(const char *group_file_name, const char *section_name, const char *key, const char *value,
                           int change_type = CHANGE_FILE_MODIFY, char key_delimiter = '=');

    static void change_file_manipulate_kv(const std::string &file_name, file_util::FileUtilChangeType op_cmd,
                                          const std::string &section_name, char kv_delimiter,
                                          const std::vector<std::string> keys, const std::vector<std::string> values,
                                          std::vector<file_util::OpResult> &results);
};

class coding_util {
public:
    static void encode_fixed32(char *buf, uint32_t value) {
        buf[0] = value & 0xff;
        buf[1] = (value >> 8) & 0xff;
        buf[2] = (value >> 16) & 0xff;
        buf[3] = (value >> 24) & 0xff;
    }

    static uint32_t decode_fixed32(const char *buf) {
        return ((static_cast<uint32_t>(static_cast<unsigned char>(buf[0])))
                | (static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 8)
                | (static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 16)
                | (static_cast<uint32_t>(static_cast<unsigned char>(buf[3])) << 24));
    }

    static void encode_fixed64(char *buf, uint64_t value) {
        buf[0] = value & 0xff;
        buf[1] = (value >> 8) & 0xff;
        buf[2] = (value >> 16) & 0xff;
        buf[3] = (value >> 24) & 0xff;
        buf[4] = (value >> 32) & 0xff;
        buf[5] = (value >> 40) & 0xff;
        buf[6] = (value >> 48) & 0xff;
        buf[7] = (value >> 56) & 0xff;
    }

    static uint64_t decode_fixed64(const char *buf) {
        return (static_cast<uint64_t>(decode_fixed32(buf + 4)) << 32) | decode_fixed32(buf);
    }
};

// avoid boost::dynamic_bitset use, implement a simple dynamic_bit by std::vector<bool> (stl_bvector)
class dynamic_bitset {
public:
    dynamic_bitset() {}

    explicit dynamic_bitset(size_t n) : set_(n, false) {}

    dynamic_bitset(const dynamic_bitset &bs) : set_(bs.set_) {}

    ~dynamic_bitset() {}

    void reset() {
        for (size_t i = 0; i < set_.size(); ++i) {
            set_[i] = false;
        }
    }

    void set(size_t pos, bool val = true) {
        if (pos < set_.size()) {
            set_[pos] = val;
        }
    }

    bool test(size_t pos) const {
        return pos < set_.size() ? set_[pos] : false;
    }

    bool all() const {
        for (size_t i = 0; i < set_.size(); ++i) {
            if (!set_[i]) {
                return false;
            }
        }
        return true;
    }

    bool any() const {
        for (size_t i = 0; i < set_.size(); ++i) {
            if (set_[i]) {
                return true;
            }
        }
        return false;
    }

    // reserve original bit flag
    void resize(size_t new_size) {
        size_t old_size = set_.size();
        set_.resize(new_size);
        for (size_t i = old_size; i < set_.size(); ++i) {
            set_[i] = false;
        }
    }

    size_t size() const {
        return set_.size();
    }

private:
    std::vector<bool> set_;
};

class MM {
public:
    static void malloc_stats() {
        ::malloc_stats();
    }

    static void release_free_memory() {
#ifdef WITH_TCMALLOC
        MallocExtension::instance()->ReleaseFreeMemory();
        log_warn("free memory in TCMalloc released");
#endif
    }
};

//check boundary.
//return true, if the `value belong to the range [low_bound, upper_bound],
//otherwise return false.
inline bool boundary_available(int value, int low_bound, int upper_bound) {
    return low_bound <= upper_bound && low_bound <= value && value <= upper_bound;
}

class StaticBitMap {
public:
    StaticBitMap(int size) {
        size_ = size;
        used_size_ = ((size + 0x07) >> 3) << 3;
        bitmap_ = new unsigned char[used_size_];
        memset(bitmap_, 0, sizeof(char) * used_size_);
    }

    ~StaticBitMap() {
        delete[]bitmap_;
        bitmap_ = NULL;
    }

    inline bool test(int index) {
        return (bitmap_[index >> 3] & (1 << (index & 0x07)));
    }

    inline void on(int index) {
        unsigned char b = 0;
        unsigned char c = 0;
        int idx = index >> 3;
        int offset = index & 0x07;
        do {
            b = bitmap_[idx];
            c = b | (1 << offset);
        } while (b != common::atomic_compare_exchange(bitmap_ + idx, c, b));
    }

    inline void off(int index) {
        unsigned char b = 0;
        unsigned char c = 0;
        int idx = index >> 3;
        unsigned char offset = index & 0x07;
        do {
            b = bitmap_[idx];
            c = b & ((~(1 << offset)) & 0xff);
        } while (b != common::atomic_compare_exchange(bitmap_ + idx, c, b));
    }

    inline int size() {
        return size_;
    }

    inline StaticBitMap *clone() {
        unsigned char *b = new unsigned char[used_size_];
        memcpy(b, bitmap_, used_size_);
        return new StaticBitMap(b, size_, used_size_);
    }

private:
    StaticBitMap(unsigned char *b, int size, int used_size) {
        bitmap_ = b;
        size_ = size;
        used_size_ = used_size;
    }

private:
    int size_;
    int used_size_;
    unsigned char *bitmap_;
};

class BitMap {
public:
    BitMap(char *data, int32_t count, int32_t cursor = 0) :
            data_(data), count_(count), cursor_(cursor) {
    }

    ~BitMap() {}

    bool test(int32_t p) {
        return (data_[p / SLOT_SIZE] & mask_[p % SLOT_SIZE]) != 0;
    }

    void set(int32_t p) {
        data_[p / SLOT_SIZE] |= mask_[p % SLOT_SIZE];
        // last set position
        cursor_ = p;
    }

    void reset() {
        memset(data_, 0, byte_size());
    }

    void reset(int32_t p) {
        data_[p / SLOT_SIZE] &= ~mask_[p % SLOT_SIZE];
    }

    int32_t pick() {
        // we consider next position of last set() as un-set
        int32_t i = (cursor_ + 1) % count_;
        while (i != cursor_) {
            if (!test(i)) {
                break;
            }
            i++;
            if (i >= count_) {
                i = 0;
            }
        }
        return (i == cursor_) ? -1 : i;
    }

    int32_t size() {
        return count_;
    }

    int32_t byte_size() {
        return byte_size(count_);
    }

    static int32_t byte_size(int32_t count) {
        return (count + count - 1) / SLOT_SIZE;
    }

    char *data() {
        return data_;
    }

private:
    char *data_;
    int32_t count_;
    int32_t cursor_;          // last set position

    static const int32_t SLOT_SIZE = 8;
    static const unsigned char mask_[SLOT_SIZE];
};

class Crematory {
public:
    Crematory() : last_fire_time_(0) {
    }

    ~Crematory() {
        fire(true);
    }

    typedef void (*CleanupFunc)(void *trash);

    void add(void *trash, CleanupFunc cleanup) {
        tbsys::CThreadGuard guard(&lock_);
        bodys_.push_back(new Body(time(NULL), trash, cleanup));
    }

    // not want to be self-thread here, somebody should call me.
    void fire(bool force = false) {
        if (!bodys_.empty() && (force || time(NULL) - last_fire_time_ > Body::ALIVE_TIME_S)) {
            // avoid side-effect of cleanup
            std::vector<Body *> deads;
            {
                tbsys::CThreadGuard guard(&lock_);
                time_t now = time(NULL);
                std::list<Body *>::iterator it = bodys_.begin();
                while (it != bodys_.end() && (force || (*it)->dead(now))) // list is time-sorted
                {
                    deads.push_back(*it);
                    it = bodys_.erase(it);
                }
                last_fire_time_ = time(NULL);
            }

            for (size_t i = 0; i < deads.size(); ++i) {
                delete deads[i];
            }
        }
    }

public:
    static Crematory *g_crematory;

private:
    typedef struct Body {
        Body(time_t time, void *trash, CleanupFunc cleanup) :
                dead_time_(time + ALIVE_TIME_S), trash_(trash), cleanup_(cleanup) {}

        ~Body() {
            cleanup_(trash_);
        }

        bool dead(time_t now) {
            return now > dead_time_;
        }

        time_t dead_time_;
        void *trash_;
        CleanupFunc cleanup_;

        static const time_t ALIVE_TIME_S = 10;
    } Body;

    tbsys::CThreadMutex lock_;
    std::list<Body *> bodys_;
    time_t last_fire_time_;
};

#define LAZY_DESTROY(trash, cleanup) do { tair::util::Crematory::g_crematory->add(trash, cleanup); } while (0)
#define TRY_DESTROY_TRASH() do { tair::util::Crematory::g_crematory->fire(); } while (0)
}

}
#endif
