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

#ifndef TAIR_COMMON_STAT_DEFINE_H
#define TAIR_COMMON_STAT_DEFINE_H

#include <stdint.h>

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std
{
    using tr1::hash;
    using tr1::unordered_map;
}
#endif

#include <set>
#include <string>

#include <tbsys.h>

#include "define.hpp"
#include "util.hpp"
#include "tair_atomic.hpp"

namespace tair {
namespace common {
template<typename T>
void any_cleanup(void *a) {
    delete reinterpret_cast<T>(a);
}

// Stat data is actually char data
typedef char Stat;

//////////////////// StatDesc
// one type statistics description.
typedef struct StatDesc {
    int32_t type_;
    int32_t sizeof_;
    const char *desc_;
} StatDesc;

// default key description
static const StatDesc default_key_stat_desc = {0, 4, "area"};

// default byteorder
#if __BYTE_ORDER == __LITTLE_ENDIAN
static const char default_byteorder = '0';
#else
static const char default_byteorder = '1';
#endif
//////////////////////////////

//////////////////// StatSchema
// statistics schema information, we need this to parse specified statistics.
// NOTE:
//  We serialize all statistics in ROW-COLUMN table way,
//  StatSchema itself is serialized in the very beginning.
//  Now, all statistics value(including key) is integer(self-defined byte-size),
//  and is serialized in binary way(string-format may be more human-friendly, but
//  cost more time/space when serializing).
//  Every row has one key which is the first column, following columns are key's
//  all type statistics.
//  Format is like:
//
//    [StatSchema]
//    [column0][column1][column2]...[columnN]
//    [column0][column1][column2]...[columnN]
//             ........
//    [column0][column1][column2]...[columnN]
//
//  StatSchema's format is like:
//
//    [byteorder][column_count][desc0_size][desc0][desc1_size][desc1]...[column0_sizeof][column1_sizeof]...
//
//  `column_count is just column size of one row(including key
//  so desc0 is key's description and column0 is the very key).
class StatSchema {
public:
    typedef int32_t (*EncodeIntegerFunc)(char *pos, const void *value);

    typedef int32_t (*DecodeIntegerFunc)(const char *pos, void *value);

    typedef int32_t (*ResetIntegerFunc)(char *pos, void *value, const void *new_value);

    typedef void (*UpdateIntegerFunc)(void *base, int64_t value);

    struct Func {
        Func() :
                offset_(0), encode_(NULL), decode_(NULL), update_(NULL) {}

        Func(int32_t offset, EncodeIntegerFunc encode_func, DecodeIntegerFunc decode_func,
             ResetIntegerFunc reset_func, UpdateIntegerFunc update_func) :
                offset_(offset), encode_(encode_func), decode_(decode_func), reset_(reset_func), update_(update_func) {}

        // offset in stat data
        int32_t offset_;
        EncodeIntegerFunc encode_;
        DecodeIntegerFunc decode_;
        ResetIntegerFunc reset_;
        UpdateIntegerFunc update_;
    };

public:
    StatSchema();

    StatSchema(const StatDesc *all_stat_descs, int32_t size, int32_t skip_type = 0,
               const StatDesc *key_desc = &default_key_stat_desc, char byteorder = default_byteorder);

    ~StatSchema();

    // merge schema to mine
    void merge(const StatSchema &schema);

    int32_t offset(int32_t type) const;

    int32_t stat_type_size() const;

    int32_t key_size() const;

    // one row stat size
    int32_t one_row_stat_size() const;

    // one row size(including key) when serializing
    int32_t one_row_size() const;

    // max size needed to serialize row size stat
    int32_t max_serialized_size(int32_t size) const;

    // one row of all 0 value(NOT including key)
    const std::string &get_placeholder() const;

    // schema serializd string
    const std::string &to_string() const;

    // serialize key => stats to buf, return serialized size
    int32_t serialize_one_row_stat(char *buf, int32_t key, Stat *stats) const;

    // set value
    void set_value(Stat *s, int32_t type, int64_t value) const;

    // get value
    int64_t get_value(Stat *s, int32_t type) const;

    // update
    void update(Stat *s, int32_t type, int32_t value) const;

    // add diff to base
    void update(Stat *base, const Stat *diff, int32_t type) const;

    void update(Stat *base, const Stat *diff, const std::vector<int32_t> *types = NULL) const;

    // reset stat
    void reset(Stat *s, int32_t score, int32_t type) const;

    void reset(Stat *s, int32_t score, const std::vector<int32_t> *types = NULL) const;

    // compare one row stat(based on key)
    int cmp_stat(const char *a, const char *b) const;

    // merge two separated serialized stat data to one.
    static void merge_stats(int32_t max_row,
                            const StatSchema &schema_a, const char *stat_buf_a, int32_t stat_size_a,
                            const StatSchema &schema_b, const char *stat_buf_b, int32_t stat_size_b,
                            const StatSchema &schema, char *&stat_buf, int32_t &stat_buf_size, int32_t &stat_data_size);
    /*
    static void debug_stat(const StatSchema& schema, const char* stat_buf, int32_t stat_size);
    */

    // funcs
    static int32_t encode_fixed8(char *pos, const void *value);

    static int32_t decode_fixed8(const char *pos, void *value);

    static int32_t reset_fixed8(char *pos, void *value, const void *new_value);

    static void update_fixed8(void *base, int64_t value);

    static int32_t encode_fixed16(char *pos, const void *value);

    static int32_t decode_fixed16(const char *pos, void *value);

    static int32_t reset_fixed16(char *pos, void *value, const void *new_value);

    static void update_fixed16(void *base, int64_t value);

    static int32_t encode_fixed32(char *pos, const void *value);

    static int32_t decode_fixed32(const char *pos, void *value);

    static int32_t reset_fixed32(char *pos, void *value, const void *new_value);

    static void update_fixed32(void *base, int64_t value);

    static int32_t encode_fixed64(char *pos, const void *value);

    static int32_t decode_fixed64(const char *pos, void *value);

    static int32_t reset_fixed64(char *pos, void *value, const void *new_value);

    static void update_fixed64(void *base, int64_t value);

private:
    void serialize_schema();

private:
    // encoding byteorder of all stat value
    char byteorder_;

    // followings are each column's meta,
    // key is at the end for convenient use by real stat column.

    // sizeof(each column data) after being serialized
    std::vector<int32_t> sizeofs_;
    // each column description
    std::vector<std::string> descs_;
    // each column function, funcs are used when update()/serialize().
    std::vector<Func> funcs_;
    // real valid column size(hole type exist, maybe)
    int32_t column_size_;

    // some information for convenient use
    // schema string
    std::string schema_str_;
    // one row size(serialized data)
    int32_t one_row_size_;
    // one row stat size
    int32_t one_row_stat_size_;
    // one row stat placeholder
    std::string placeholder_;
};

// for deserialize StatSchema use
class DeStatSchema {
public:
    DeStatSchema();

    ~DeStatSchema();

    bool init(const char *schema_string, int32_t len, int32_t version);

    void clear();

    bool deserialize(const char *buf, int32_t buf_len, std::vector<int64_t> &value);

    int32_t get_version() { return version_; }

    int32_t get_column_size() { return (int32_t) descs_.size(); }

    const std::vector<std::string> &get_desc() { return descs_; }

private:
    static int64_t decode(const char *pos, int32_t size);

    static int64_t decode_reverse(const char *pos, int32_t size);

private:
    // sizeof(each column data) after being serialized
    std::vector<int32_t> sizeofs_;
    // each column description
    std::vector<std::string> descs_;
    // one row size(serialized data)
    int32_t one_row_size_;
    int32_t version_;

    int64_t (*decode_func_)(const char *pos, int32_t size);
};

//////////////////////////////

//////////////////// StatStore
// store statistics information.
// Actually, StatUnit can use StatStore in template way,
// however, we still use base class inheritance
// to be able to pass different StatStore to one StatUnit for
// special purpose (StatUnit::update() eg.).
class StatStore {
public:
    StatStore() {}

    virtual ~StatStore() {}

    // new one instance base on index
    virtual StatStore *clone(int32_t index) = 0;

    virtual int start() = 0;

    virtual int stop() = 0;

    virtual int destroy() = 0;

    virtual Stat *add_stat(int32_t key) = 0;

    // (drop == true) means this statunit is out of need, never and ever
    virtual void clear_stat(Stat *s, bool drop = true) = 0;

    // iterate all stat, return false if all have done.
    // maybe StatStore::Iterator implementation looks better.
    virtual void seek_stat() {}

    virtual bool next_stat(int32_t &key, Stat *&s) { return false; }

    int32_t max_serialized_size(int32_t size) { return schema_->max_serialized_size(size); }

    const StatSchema *get_schema() { return schema_; }

protected:
    const StatSchema *schema_;
};

//////////////////// MemStatStore
// store statistics in memory way
class MemStatStore : public StatStore {
public:
    MemStatStore(const StatSchema *schema);

    ~MemStatStore();

    StatStore *clone(int32_t index);

    int start();

    int stop();

    int destroy();

    Stat *add_stat(int32_t key);

    void clear_stat(Stat *s, bool drop = true);
};

//////////////////// DiskStatStore
// store statistics in persistence way
class DiskStatStore : public StatStore {
public:
#pragma pack(4)
    // storage struct
    typedef struct {
        int32_t version_;
        int32_t max_unit_count_;
    } Header;

    typedef struct Unit {
        // for make stat_ align to 8
        int64_t key_;
        Stat stat_[0];

        static std::string to_string(const Stat *s, int32_t stat_size) {
            std::string result;
            result.reserve(stat_size * 2);
            char buf[4];
            for (int i = 0; i < stat_size; ++i) {
                snprintf(buf, sizeof(buf), "%x", s[i]);
                result.append(buf);
            }
            return result;
        }

        static int32_t byte_size(int32_t stat_size) {
            //up bound to multiple of 8
            return (stat_size + sizeof(int64_t) + (8 - 1)) & (~(8 - 1));
        }

        static Unit *stat_to_unit(Stat *s) {
            return reinterpret_cast<Unit *>(s - sizeof(int64_t));
        }
    } Unit;
#pragma pack()

public:
    DiskStatStore(const StatSchema *schema, int32_t version, int32_t max_unit_count, const char *path);

    ~DiskStatStore();

    StatStore *clone(int32_t index);

    int start();

    int stop();

    int destroy();

    Stat *add_stat(int32_t key);

    void clear_stat(Stat *s, bool drop = true);

    void seek_stat();

    bool next_stat(int32_t &key, Stat *&s);

private:
    int build_stat();

    int build_stat_from_exist(int fd);

    int build_stat_from_new(int fd);

    Header *get_header();

    char *get_bitmap_data();

    Unit *get_unit(int32_t index);

    int32_t get_unit_index(Unit *u);

    int flush(bool meta_only = true);

private:
    int32_t version_;
    int32_t max_unit_count_;

    int32_t unit_size_;
    Unit *unit_start_;

    char path_[TAIR_MAX_PATH_LEN];
    char *mmap_data_;
    int64_t mmap_size_;
    util::BitMap *bitmap_;

    // for iterator
    int32_t iter_;
};


//////////////////// StatUnit
// One key's statistics unit
template<typename Store>
class StatUnit {
public:
    StatUnit(Stat *stat);

    ~StatUnit();

    // update stat with `diff from this StatUnit's offset
    void
    update(const StatUnit<Store> &diff, const std::vector<int32_t> *types, int32_t offset, const StatSchema *schema);

    void reset(int32_t score, const std::vector<int32_t> *types, const StatSchema *schema);

    Stat *get_stat();

    static StatUnit *new_instance(Store *store, Stat *s);

    static StatUnit *new_instance(Store *store, int32_t key);

    // (drop == true) means this statunit is out of need, never and ever
    static void destroy_instance(Store *store, StatUnit *su, bool drop = true);

private:
    Stat *stat_;
};

template<typename Store>
StatUnit<Store>::StatUnit(Stat *stat) :
        stat_(stat) {
}

template<typename Store>
StatUnit<Store>::~StatUnit() {
}

template<typename Store>
void StatUnit<Store>::update(const StatUnit<Store> &diff, const std::vector<int32_t> *types,
                             int32_t offset, const StatSchema *schema) {
    schema->update(stat_ + offset, diff.stat_, types);
}

template<typename Store>
void StatUnit<Store>::reset(int32_t score, const std::vector<int32_t> *types, const StatSchema *schema) {
    schema->reset(stat_, score, types);
}

template<typename Store>
Stat *StatUnit<Store>::get_stat() {
    return stat_;
}

template<typename Store>
StatUnit<Store> *StatUnit<Store>::new_instance(Store *store, Stat *s) {
    UNUSED(store);
    return new StatUnit<Store>(s);
}

template<typename Store>
StatUnit<Store> *StatUnit<Store>::new_instance(Store *store, int32_t key) {
    StatUnit<Store> *su = NULL;
    Stat *s = store->add_stat(key);
    if (s != NULL) {
        su = new StatUnit<Store>(s);
    }

    return su;
}

template<typename Store>
void StatUnit<Store>::destroy_instance(Store *store, StatUnit<Store> *su, bool drop) {
    if (NULL != su) {
        store->clear_stat(su->stat_, drop);
        LAZY_DESTROY(su, (any_cleanup<StatUnit<Store> *>));
    }
}

//////////////////// StatManager
// manage all statistics, mainly construct Key ==> StatUnit map relationship.
template<typename StatUnit, typename StatStore>
class StatManager {
public:
    explicit StatManager(StatStore *store);

    ~StatManager();

    // start statmanager.
    // maybe some statistics should be loaded at startup.
    void start();

    // set `type statistics of `key with `value
    void set_value(int32_t key, int32_t type, int64_t value);

    // get `type statistics of `key
    int64_t get_value(int32_t key, int32_t type);

    // update `type statistics of `key with `value
    void update(int32_t key, int32_t type, int32_t value);

    // update `types stat with `sm from this StatManager's offset, and `schema
    void update(const StatManager<StatUnit, StatStore> &sm,
                const std::vector<int32_t> *types = NULL, int32_t offset = 0, const StatSchema *schema = NULL);

    // reset all statunit with score
    void reset(int32_t score = 0, const std::vector<int32_t> *types = NULL, const StatSchema *schema = NULL);

    // reset only one statunit of key with score
    void reset_key(int32_t key, int32_t score = 0, const std::vector<int32_t> *types = NULL,
                   const StatSchema *schema = NULL);

    // clear key => stat
    void clear(int32_t key);

    // get statistics data of `key, user may want to update
    // several specified key's stats, get stat data directly and
    // update with `schema later to avoid redundant key-stat-find.
    Stat *get_stat(int32_t key);

    // serialize all statistics, `alloc indicate whether user
    // need defree buf.
    // Now, serialize is not (no need) thread-safe..
    void serialize(char *&buf, int32_t &size, bool &alloc);

    // sampling all statics to `result_stat
    void sampling(StatUnit &result_stat);

    // `result_stat may have more stats than `this, so sampling `this to `result_stat from `result_stat's `offset_type
    void sampling(StatManager<StatUnit, StatStore> &result_stat, int32_t offset_type = 0);

private:
    // add one StatUnit of key, return result.
    StatUnit *add_statunit(int32_t key);

    void clear_statunit(int32_t key);

private:
    typedef struct {
        int32_t key;
        StatUnit *su;
    } KeyAndStat;

    // lock guard when update stats_
    tbsys::CRWLock *lock_;
    StatUnit *stats_[TAIR_MAX_AREA_COUNT];

    // all StatUnits' store
    StatStore *store_;
    // all StatUnits' schema
    const StatSchema *schema_;

    // whether sorted_stats_ need update
    bool stats_updated_;
    std::vector<KeyAndStat> exist_key_stat;

    // serialized data
    char *data_;
    // data buf size
    int32_t data_buf_size_;
};

template<typename StatUnit, typename StatStore>
StatManager<StatUnit, StatStore>::StatManager(StatStore *store) :
        lock_(new tbsys::CRWLock()), store_(store), schema_(store->get_schema()),
        stats_updated_(true), data_(NULL), data_buf_size_(0) {
    for (int32_t i = 0; i < TAIR_MAX_AREA_COUNT; i++)
        stats_[i] = NULL;
}

template<typename StatUnit, typename StatStore>
StatManager<StatUnit, StatStore>::~StatManager() {
    for (int32_t i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
        StatUnit *su = stats_[i];
        if (su != NULL)
            StatUnit::destroy_instance(store_, su, false/*just delete, not drop*/);
    }

    // delete StatStore
    delete store_;
    delete lock_;

    if (data_ != NULL) {
        delete[] data_;
        data_ = NULL;
    }
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::start() {
    tbsys::CWLockGuard guard(*lock_);
    // we need load all stat in StatStore at first.
    int32_t key;
    Stat *s = NULL;
    store_->seek_stat();
    while (store_->next_stat(key, s)) {
        StatUnit *old_su = stats_[key];
        if (UNLIKELY(old_su != NULL)) {
            // TODO: log
            log_error("conflict stat find, use latest");
            StatUnit::destroy_instance(store_, old_su);
        }

        stats_[key] = StatUnit::new_instance(store_, s);
    }
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::set_value(int32_t key, int32_t type, int64_t value) {
    StatUnit *su = stats_[key];
    if (UNLIKELY(NULL == su))
        su = add_statunit(key);  //there's a rwlock_w in add_statunit to prevent multi writer
    schema_->set_value(su->get_stat(), type, value);
}

template<typename StatUnit, typename StatStore>
int64_t StatManager<StatUnit, StatStore>::get_value(int32_t key, int32_t type) {
    StatUnit *su = stats_[key];
    if (UNLIKELY(NULL == su))
        return 0;
    else
        return schema_->get_value(su->get_stat(), type);
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::update(int32_t key, int32_t type, int32_t value) {
    StatUnit *su = stats_[key];
    if (UNLIKELY(NULL == su))
        su = add_statunit(key); //there's a rwlock_w in add_statunit to prevent multi writer
    schema_->update(su->get_stat(), type, value);
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::update(const StatManager<StatUnit, StatStore> &diff,
                                              const std::vector<int32_t> *types, int32_t offset_type,
                                              const StatSchema *schema) {
    if (schema == NULL)
        schema = schema_;

    // get my actual stat offset
    int32_t offset = schema_->offset(offset_type);
    for (int32_t key = 0; key < TAIR_MAX_AREA_COUNT; ++key) {
        StatUnit *diff_su = diff.stats_[key];
        if (diff_su != NULL) {
            StatUnit *su = stats_[key];
            if (UNLIKELY(NULL == su))
                su = add_statunit(key);
            su->update(*diff_su, types, offset, schema);
        }// end of if (diff_su != NULL)
    }
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::reset(int32_t score, const std::vector<int32_t> *types,
                                             const StatSchema *schema) {
    if (schema == NULL)
        schema = schema_;

    for (int32_t key = 0; key < TAIR_MAX_AREA_COUNT; ++key) {
        StatUnit *su = stats_[key];
        if (NULL != su)
            su->reset(score, types, schema);
    }
}

// reset only one unitstat of key
template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::reset_key(int32_t key, int32_t score, const std::vector<int32_t> *types,
                                                 const StatSchema *schema) {
    if (!(0 <= key && key <= TAIR_MAX_AREA_COUNT))
        return;
    if (schema == NULL)
        schema = schema_;

    StatUnit *su = stats_[key];
    if (NULL != su)
        su->reset(score, types, schema);
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::clear(int32_t key) {
    clear_statunit(key);
}

template<typename StatUnit, typename StatStore>
Stat *StatManager<StatUnit, StatStore>::get_stat(int32_t key) {
    StatUnit *su = stats_[key];
    if (UNLIKELY(NULL == su))
        su = add_statunit(key);
    return su->get_stat();
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::serialize(char *&buf, int32_t &size, bool &alloc) {
    // check sort
    if (stats_updated_) {
        tbsys::CWLockGuard guard(*lock_);
        if (stats_updated_) {
            exist_key_stat.clear();
            for (int32_t key = 0; key < TAIR_MAX_AREA_COUNT; ++key) {
                StatUnit *su = stats_[key];
                if (su != NULL) {
                    KeyAndStat tmp;
                    tmp.key = key;
                    tmp.su = su;
                    exist_key_stat.push_back(tmp);
                }
            }
            stats_updated_ = false;
        }
    }

    // check data buffer
    tbsys::CRLockGuard guard(*lock_);
    int32_t max_size = store_->max_serialized_size(exist_key_stat.size());
    if (UNLIKELY(max_size > data_buf_size_)) {
        if (data_ != NULL) {
            delete[] data_;
            data_ = NULL;
        }
        data_buf_size_ = max_size;
        data_ = new char[data_buf_size_];
    }

    char *pos = data_;

    for (typename std::vector<KeyAndStat>::const_iterator it = exist_key_stat.begin();
         it != exist_key_stat.end(); ++it)
        pos += schema_->serialize_one_row_stat(pos, it->key, it->su->get_stat());

    buf = data_;
    size = pos - buf;
    alloc = false;
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::sampling(StatUnit &result_stat) {
    for (int32_t key = 0; key < TAIR_MAX_AREA_COUNT; ++key) {
        StatUnit *su = stats_[key];
        if (su != NULL)
            result_stat.update(su, NULL, 0, schema_);
    }
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::sampling(StatManager<StatUnit, StatStore> &result_stat,
                                                int32_t offset_type) {
    // result_stat's schema is larger than schema_, use mine here
    result_stat.update(*this, NULL, offset_type, schema_);
}

template<typename StatUnit, typename StatStore>
StatUnit *StatManager<StatUnit, StatStore>::add_statunit(int32_t key) {
    StatUnit *su = stats_[key];
    if (NULL == su) {
        tbsys::CWLockGuard guard(*lock_);
        su = stats_[key];
        if (NULL == su) {
            su = StatUnit::new_instance(store_, key);
            stats_[key] = su;
            stats_updated_ = true;
        }
    }
    return su;
}

template<typename StatUnit, typename StatStore>
void StatManager<StatUnit, StatStore>::clear_statunit(int32_t key) {
    tbsys::CWLockGuard guard(*lock_);
    StatUnit *su = stats_[key];
    if (NULL != su) {
        stats_[key] = NULL;
        StatUnit::destroy_instance(store_, su);
        stats_updated_ = true;
    }
}

//////////////////// OpStat
// operation statistics

// NOTE: DO NOT assign specified value to any following enums,
//       just enjoy the beauty of standing-in-a-row.
typedef enum {
    OP_PUT = 0,   // put + mput + set_count
    OP_PUT_KEY,   // put + nr(mput) + set_count, nr(mput) means the key num in mput packet.
    OP_GET,       // get + mget
    OP_GET_KEY,   // get + nr(mget)
    OP_HIT,
    OP_REMOVE_KEY,   // remove + invalidate + nr(removes) + nr(mdelete) + nr(minvalid)
    OP_HIDE,
    OP_ADD,        //incr + decr + add_count
    OP_PF_PUT,    // prefix_put + prefix_puts
    OP_PF_PUT_KEY,  // prefix_put + nr(prefix_puts)
    OP_PF_GET,    // prefix_get + prefix_gets
    OP_PF_GET_KEY,   // prefix_get + nr(prefix_gets)
    OP_PF_HIT,    // prefix_hit
    OP_PF_REMOVE_KEY, // prefix_remove + prefix_invalidate + nr(prefix_removes) + nr(del_range)
    OP_PF_HIDE,   //prefix_hide
    OP_GET_RANGE, //get_range
    OP_GET_RANGE_KEY, // nr(get_range)
    OP_FC_IN,
    OP_FC_OUT,
    OP_MIGRATE_KEY, //nr(reqeust_mupdate)
    OP_MIGRATE_SIZE,//request_mupdate flow-in traffic size, byte
    // all operation statistics types MUST be less than `OP_STAT_TYPE_MAX
            OP_STAT_TYPE_MAX,
} OpStatType;

class OpStat {
public:
    OpStat();

    ~OpStat();

    const StatSchema *get_schema();

    int32_t get_type_size();

    void add(const StatSchema &extra);

    void reset(int32_t score);

    void update(int32_t area, int32_t type, int32_t value);

    void update_or_not(int32_t area, int32_t type, int32_t type_suc, int rc, int32_t value);

    Stat *get_stat(int32_t area);

    void sampling(int32_t interval_s);

    void serialize(char *&buf, int32_t &size, bool &alloc);

private:
    StatSchema schema_;

    // area => stat,
    StatManager<StatUnit<MemStatStore>, MemStatStore> *stat_;
    // last sampling stat
    StatManager<StatUnit<MemStatStore>, MemStatStore> *last_stat_;

    static const StatDesc stat_descs_[];
};

}
}
#endif
