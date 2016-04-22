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

#ifndef TAIR_STORAGE_LDB_DEFINE_H
#define TAIR_STORAGE_LDB_DEFINE_H

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "common/define.hpp"

namespace leveldb {
class DB;

class Iterator;

class LogSnapshotImpl;

class Logger;
namespace log {
class Reader;
}
}

namespace tair {
namespace storage {
namespace ldb {
const static int LDB_EXPIRED_TIME_SIZE = sizeof(uint32_t);
const static int LDB_KEY_BUCKET_NUM_SIZE = 3;
const static int LDB_KEY_META_SIZE = LDB_KEY_BUCKET_NUM_SIZE + LDB_EXPIRED_TIME_SIZE;
const static int LDB_KEY_AREA_SIZE = 2;
const static int MAX_BUCKET_NUMBER = (1 << 24) - 2;
const static int LDB_FILTER_SKIP_SIZE = LDB_EXPIRED_TIME_SIZE;
const static int LDB_KEY_CLIENT_USER_KEY_OFFSET = LDB_EXPIRED_TIME_SIZE + LDB_KEY_BUCKET_NUM_SIZE + LDB_KEY_AREA_SIZE;
const static int LDB_KEY_AREA_OFFSET = LDB_EXPIRED_TIME_SIZE + LDB_KEY_BUCKET_NUM_SIZE;

extern void ldb_key_printer(const leveldb::Slice &key, std::string &output);

extern bool get_db_stat(leveldb::DB *db, std::string &value, const char *property);

extern bool get_db_stat(leveldb::DB *db, uint64_t &value, const char *property);

extern int32_t get_level_num(leveldb::DB *db);

extern bool get_level_range(leveldb::DB *db, int32_t level, std::string *smallest, std::string *largest);

extern std::string get_back_path(const char *path);

enum {
    LDB_TYPE_UPDATE,
    LDB_TYPE_APPEND,
    LDB_TYPE_PREPEND,
    LDB_TYPE_ADD,
    LDB_TYPE_TOUCH,
};

template<class T>
void destroy_container(T **&container, int32_t count) {
    if (container != NULL) {
        for (int32_t i = 0; i < count; ++i) {
            delete container[i];
        }
        delete[] container;
        container = NULL;
    }
}

inline std::string append_num(const std::string &base, int32_t num) {
    char buf[16];
    snprintf(buf, sizeof(buf), ".%d", num);
    return base + buf;
}

extern bool
do_file_repair_check(leveldb::Iterator *iter, leveldb::ManualCompactionType type, leveldb::Logger *info_log);

class LdbKey {
public:
    LdbKey() : data_(NULL), data_size_(0), alloc_(false) {
    }

    LdbKey(const char *key_data, int32_t key_size, int32_t bucket_number, uint32_t expired_time = 0) :
            data_(NULL), data_size_(0), alloc_(false) {
        set(key_data, key_size, bucket_number, expired_time);
    }

    ~LdbKey() {
        free();
    }

    inline void build_key_meta(int32_t bucket_number, uint32_t expired_time = 0) {
        // consistent key len SCAN_KEY_LEN
        // big-endian to use default bitewise comparator.
        // consider: varintint may be space-saved,
        // but user defined comparator need caculate datalen every time.

        // encode expired time
        build_key_meta(data_, bucket_number, expired_time);
    }

    inline void set(const char *key_data, int32_t key_size, int32_t bucket_number, uint32_t expired_time) {
        if (key_data != NULL && key_size > 0) {
            free();
            data_size_ = key_size + LDB_KEY_META_SIZE;
            data_ = new char[data_size_];
            build_key_meta(data_, bucket_number, expired_time);
            memcpy(data_ + LDB_KEY_META_SIZE, key_data, key_size);
            alloc_ = true;
        }
    }

    inline void assign(char *data, const int32_t data_size) {
        free();
        data_ = data;
        data_size_ = data_size;
        alloc_ = false;
    }

    inline void free() {
        if (alloc_ && data_ != NULL) {
            delete[] data_;
            data_ = NULL;
            data_size_ = 0;
            alloc_ = false;
        }
    }

    inline char *data() {
        return data_;
    }

    inline int32_t size() {
        return data_size_;
    }

    inline char *key() {
        return data_ != NULL ? data_ + LDB_KEY_META_SIZE : NULL;
    }

    inline int32_t key_size() {
        return data_size_ > LDB_KEY_META_SIZE ? (data_size_ - LDB_KEY_META_SIZE) : 0;
    }

    inline void incr_key(int index) {
        index += LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE - 1;
        while (index > 0)
            if ((uint8_t) data_[index] != 0xFF) {
                data_[index]++;
                break;
            } else
                index--;
    }

    static void build_key_meta(char *buf, int32_t bucket_number, uint32_t expired_time = 0) {
        // consistent key len SCAN_KEY_LEN
        // big-endian to use default bitewise comparator.
        // consider: varintint may be space-saved,
        // but user defined comparator need caculate datalen every time.

        // encode expired time
        tair::util::coding_util::encode_fixed32(buf, expired_time);
        encode_bucket_number(buf + LDB_EXPIRED_TIME_SIZE, bucket_number);
    }

    static void encode_bucket_number(char *buf, int bucket_number) {
        for (int i = 0; i < LDB_KEY_BUCKET_NUM_SIZE; ++i) {
            buf[LDB_KEY_BUCKET_NUM_SIZE - i - 1] = (bucket_number >> (i * 8)) & 0xFF;
        }
    }

    inline int32_t get_bucket_number() {
        return decode_bucket_number(data_ + LDB_EXPIRED_TIME_SIZE);
    }

    static int32_t get_bucket_number(const char *data) {
        return decode_bucket_number(data + LDB_EXPIRED_TIME_SIZE);
    }

    static int32_t decode_bucket_number_with_key(const char *key_buf) {
        return decode_bucket_number(key_buf + LDB_EXPIRED_TIME_SIZE);
    }

    static int32_t decode_bucket_number(const char *buf) {
        int bucket_number = 0;
        for (int i = 0; i < LDB_KEY_BUCKET_NUM_SIZE; ++i) {
            bucket_number |=
                    static_cast<int32_t>(static_cast<unsigned char>(buf[i])) << ((LDB_KEY_BUCKET_NUM_SIZE - i - 1) * 8);
        }
        return bucket_number;
    }

    static void encode_area(char *buf, int32_t area) {
        buf[0] = area & 0xff;
        buf[1] = (area >> 8) & 0xff;
    }

    static int32_t decode_area(const char *buf) {
        return (static_cast<int32_t>(static_cast<unsigned char>(buf[1])) << 8) | static_cast<unsigned char>(buf[0]);
    }

    static int32_t decode_expire_with_key(const char *key_buf) {
        return tair::util::coding_util::decode_fixed32(key_buf);
    }

    static int32_t decode_area_with_key(const char *key_buf) {
        return decode_area(key_buf + LDB_KEY_META_SIZE);
    }

    static void build_scan_key(int32_t bucket_number, std::string &start_key, std::string &end_key) {
        char buf[LDB_KEY_META_SIZE];
        build_key_meta(buf, bucket_number);
        start_key.assign(buf, LDB_KEY_META_SIZE);
        build_key_meta(buf, bucket_number + 1);
        end_key.assign(buf, LDB_KEY_META_SIZE);
    }

    static void
    build_scan_key_with_area(int32_t bucket_number, int32_t area, std::string &start_key, std::string &end_key) {
        char buf[LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE];

        build_key_meta(buf, bucket_number);
        encode_area(buf + LDB_KEY_META_SIZE, area);
        start_key.assign(buf, sizeof(buf));

        build_key_meta(buf, bucket_number);
        encode_area(buf + LDB_KEY_META_SIZE, area + 1);
        end_key.assign(buf, sizeof(buf));
    }

private:
    char *data_;
    int32_t data_size_;
    bool alloc_;
};

#pragma pack(4)

struct LdbItemMetaBase {
    LdbItemMetaBase() : meta_version_(0), flag_(0), version_(0), cdate_(0), mdate_(0), edate_(0) {}

    uint8_t meta_version_; // meta data version
    uint8_t flag_;         // flag
    uint16_t version_;      // version
    uint32_t cdate_;        // create time
    uint32_t mdate_;        // modify time
    uint32_t edate_;        // expired time(for meta when get value. dummy with key)
};

struct LdbItemMeta   // change value() and set() ,if you want to add new metadata
{
    LdbItemMeta() : prefix_size_(0) {}

    struct LdbItemMetaBase base_;
    uint16_t prefix_size_;  // prefix key size(for getRange conflict detect)
    uint16_t reserved;  //
};

#pragma pack()

const static int32_t LDB_ITEM_META_SIZE = sizeof(LdbItemMeta);
const static int32_t LDB_ITEM_META_BASE_SIZE = sizeof(LdbItemMetaBase); // add prefix_size_ at the end of the value

const static int LDB_META_SIZE = LDB_KEY_META_SIZE + LDB_ITEM_META_SIZE;

enum {
    META_VER_BASE = 0,
    META_VER_PREFIX,
};

class LdbItem {
public:
    LdbItem() : meta_(), data_(NULL), data_size_(0), alloc_(false) {

    }

    ~LdbItem() {
        free();
    }

    // meta_ MUST already be set correctly
    void set(const char *value_data, const int32_t value_size) {
        if (value_data != NULL && value_size > 0) {
            char *metap = reinterpret_cast<char *>(&meta_);
            int real_meta_size = LDB_ITEM_META_BASE_SIZE;
            LdbItemMetaBase *metabp = reinterpret_cast<LdbItemMetaBase *>(&meta_);
            free();
            if (metabp->flag_ & TAIR_ITEM_FLAG_NEWMETA) {
                if (META_VER_PREFIX == metabp->meta_version_)
                    real_meta_size = LDB_ITEM_META_SIZE;
                else if (META_VER_BASE == metabp->meta_version_)
                    real_meta_size = LDB_ITEM_META_BASE_SIZE;
            }
            data_size_ = value_size + real_meta_size;
            data_ = new char[data_size_];
            memcpy(data_, metap, real_meta_size);
            memcpy(data_ + real_meta_size, value_data, value_size);
            alloc_ = true;
        }
    }

    void assign(char *data, const int32_t data_size) {
        free();
        data_ = data;
        data_size_ = data_size;
        meta_ = *(reinterpret_cast<LdbItemMeta *>(data_));
    }

    void delegate(char *data, const int32_t data_size) {
        assign(data, data_size);
        alloc_ = true;
    }

    void free() {
        if (alloc_ && data_ != NULL) {
            delete[] data_;
            data_ = NULL;
            data_size_ = 0;
            alloc_ = false;
        }
    }

    inline char *data() {
        return data_;
    }

    inline int32_t size() {
        return data_size_;
    }

    inline int is_new_meta() {
        return meta_.base_.flag_ & TAIR_ITEM_FLAG_NEWMETA;
    }

    inline bool has_prefix() {
        return (meta_.base_.flag_ & TAIR_ITEM_FLAG_NEWMETA) && (meta_.base_.meta_version_ >= META_VER_PREFIX);
    }

    inline char *value() {
        if (NULL == data_)
            return NULL;
        if (has_prefix())
            return data_ + LDB_ITEM_META_SIZE;
        else
            return data_ + LDB_ITEM_META_BASE_SIZE;
    }

    inline int32_t value_size() {
        if (data_size_ <= LDB_ITEM_META_BASE_SIZE)
            return 0;
        if (has_prefix())
            return data_size_ - LDB_ITEM_META_SIZE;
        else
            return data_size_ - LDB_ITEM_META_BASE_SIZE;
    }

    inline LdbItemMeta &meta() {
        return meta_;
    }

    inline uint16_t prefix_size() {
        if (has_prefix())
            return meta_.prefix_size_;
        else
            return 0;
    }

    inline void set_prefix_size(uint16_t size) {
        if (has_prefix())
            meta_.prefix_size_ = size;
    }

    inline uint32_t cdate() const {
        return meta_.base_.cdate_;
    }

    inline uint32_t mdate() const {
        return meta_.base_.mdate_;
    }

    inline uint32_t edate() const {
        return meta_.base_.edate_;
    }

    inline uint16_t version() const {
        return meta_.base_.version_;
    }

    inline uint8_t flag() const {
        return meta_.base_.flag_;
    }

private:
    LdbItemMeta meta_;
    char *data_;
    int32_t data_size_;
    bool alloc_;
};


// multi-logs reader
class LdbLogsReader {
public:
    // filter log record based on key and its sequence
    class Filter {
    public:
        Filter() {}

        virtual ~Filter() {}

        virtual bool ok(int32_t type, const leveldb::Slice &key, uint64_t sequence) = 0;
    };

public:
    LdbLogsReader(leveldb::DB *db, Filter *filter = NULL, uint64_t start_lognumber = 0, bool delete_file = false);

    virtual ~LdbLogsReader();

    // get leveldb-format key/value.
    // `alloc indicates whether user need be freed
    int get_record(int32_t &type, leveldb::Slice &key, leveldb::Slice &value, bool &alloc);

private:
    int start_new_reader(uint64_t min_number);

    int init_reader(uint64_t number);

    void clear_reader(uint64_t number);

    void update_last_sequence();

    int get_log_record();

    int parse_one_kv_record(int32_t &type, leveldb::Slice &key, leveldb::Slice &value);

    bool parse_one_kv(int32_t type, leveldb::Slice &key, leveldb::Slice &value);

    bool parse_one_key(int32_t type, leveldb::Slice &key);

protected:
    leveldb::DB *db_;
    Filter *filter_;

    // whether delete log file when finish reading
    bool delete_file_;

    // current reading logfile's filenumber
    uint64_t reading_logfile_number_;
    leveldb::log::Reader *reader_;
    bool last_logfile_refed_;

    // one log record can contain several kv entry(record),
    // following is for temporary data buffer of last gotten log record
    leveldb::Slice *last_log_record_;
    std::string last_log_scratch_;
    uint64_t last_sequence_;
};

// simple iterator for one bucket's db/log data
class LdbBucketDataIter {
public:
    class LogFilter : public LdbLogsReader::Filter {
    public:
        LogFilter(int32_t bucket, uint64_t sequence) :
                bucket_(bucket), sequence_(sequence) {}

        ~LogFilter() {}

        bool ok(int32_t type, const leveldb::Slice &key, uint64_t sequence) {
            UNUSED(type);
            return (LdbKey::decode_bucket_number_with_key(key.data()) == bucket_ && sequence >= sequence_);
        }

    private:
        int32_t bucket_;
        uint64_t sequence_;
    };

public:
    LdbBucketDataIter(int32_t bucket, leveldb::DB *db);

    ~LdbBucketDataIter();

    void seek_to_first();

    // can do next() even already !valid()
    void next();

    bool valid();

    leveldb::Slice &key();

    leveldb::Slice &value();

    int32_t type();

private:
    void db_sanity();

    void clear();

private:
    int32_t bucket_;

    leveldb::DB *db_;
    leveldb::Iterator *db_it_;
    LdbLogsReader::Filter *log_filter_;
    LdbLogsReader *log_reader_;
    const leveldb::LogSnapshotImpl *log_snapshot_;

    leveldb::Slice key_;
    leveldb::Slice value_;
    int32_t type_;
    bool alloc_;
};

}
}
}
#endif
