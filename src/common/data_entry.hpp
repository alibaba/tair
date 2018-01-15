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

#ifndef TAIR_DATA_ENTRY_HPP
#define TAIR_DATA_ENTRY_HPP

#include <set>
#include "util.hpp"
#include "item_data_info.hpp"
#include "define.hpp"

namespace tair { class DataBuffer; }

namespace tair {
namespace common {
using namespace tair::util;

class data_entry {
public:
    data_entry() {
        init();
    }

    data_entry(const data_entry &entry) {
        init();

        set_data(entry.data, entry.size, entry.alloc, entry.has_merged);
        prefix_size = entry.prefix_size;
        has_merged = entry.has_merged;
        has_meta_merged = entry.has_meta_merged;
        area = entry.area;
        hashcode = entry.hashcode;
        server_flag = entry.server_flag;
        data_meta = entry.data_meta;
    }

    data_entry &operator=(const data_entry &entry) {
        if (this == &entry)
            return *this;
        set_data(entry.data, entry.size, entry.alloc, entry.has_merged);
        prefix_size = entry.prefix_size;
        has_merged = entry.has_merged;
        has_meta_merged = entry.has_meta_merged;
        area = entry.area;
        hashcode = entry.hashcode;
        server_flag = entry.server_flag;
        data_meta = entry.data_meta;

        return *this;
    }

    data_entry &clone(const data_entry &entry) {
        assert(this != &entry);

        set_data(entry.data, entry.size, true, entry.has_merged);
        prefix_size = entry.prefix_size;
        has_merged = entry.has_merged;
        has_meta_merged = entry.has_meta_merged;
        area = entry.area;
        hashcode = entry.hashcode;
        server_flag = entry.server_flag;
        data_meta = entry.data_meta;
        return *this;
    }

    bool strict_equals(const data_entry &entry) const {
        if (!((*this) == entry)) return false;
        //   uint16_t server_flag;
        //   item_meta_info data_meta;
        if (server_flag != entry.server_flag) return false;
        if (memcmp(&data_meta, &(entry.data_meta),
                   sizeof(entry.data_meta)) != 0)
            return false;
        return true;
    }

    bool operator==(const data_entry &entry) const {
        if (entry.size != size)
            return false;
        if (has_merged != entry.has_merged)
            return false;
        if (!has_merged && area != entry.area)
            return false;

        if (data == entry.data)
            return true;
        if (data == NULL || entry.data == NULL)
            return false;

        return memcmp(data, entry.data, size) == 0;
    }

    bool operator<(const data_entry &entry) const {
        if (size == 0 || entry.size == 0 || data == NULL || entry.data == NULL) return false;

        int min_size = (size > entry.size ? entry.size : size);
        int r = memcmp(data, entry.data, min_size);
        if (r < 0)
            return true;
        else if (r > 0)
            return false;
        else
            return (size < entry.size);
    }

    void to_ascii(char *b, size_t n) const {
        size_t len = 0;
        len += snprintf(b + len, n - len, "ksize: %d, prefix: %d, ns: %u, data: ",
                        get_size(), get_prefix_size(), this->area);
        for (size_t i = 0; i < (size_t) get_size() && len < n - 1; ++i) {
            if (isprint(get_data()[i])) {
                len += snprintf(b + len, n - len, "%c", (unsigned char) get_data()[i]);
            } else {
                len += snprintf(b + len, n - len, "\\%02x", (unsigned char) get_data()[i]);
            }
        }
    }

    void print_out() {
        if (size > 0) {
            char debug_data[4 * size];
            char *d = (char *) data;
            for (int j = 0; j < size; j++)
                sprintf(debug_data + 3 * j, "%02X ", (d[j] & 0xFF));
            //log_debug( "TairDataEntry(%d) - %s", size, debug_data);
        }
    }

    std::string get_printable_key(bool tohex = false) const {
#define MAX_SHOW_KEY_LEN 128
        if (size <= 0) { return "null key"; }
        if (tohex) {
            int _show_size = size < MAX_SHOW_KEY_LEN ? size : MAX_SHOW_KEY_LEN;
            int _need_size = _show_size * 3 + 5;
            char *p = (char *) malloc(_need_size);
            if (!p) return "no memory for key";

            char *d = (char *) data;

            for (int j = 0; j < _show_size; j++)
                sprintf(p + 3 * j, "%02X ", (d[j] & 0xFF));
            std::string _print_key(p);
            free(p);
            return _print_key;
        } else {
            return std::string(m_true_data, m_true_size);
        }
    }

    data_entry(const char *str, bool alloc = true) {
        init();
        set_data(str, strlen(str), alloc);
    }

    data_entry(const char *data, int size, bool alloc = true) {
        init();
        set_data(data, size, alloc);
    }

    ~data_entry() {
        if (m_true_data && alloc) {
            free(m_true_data);
        }
        //free_data();
    }

    int get_area() {
        if (has_merged) {
            this->area = (static_cast<uint32_t>(data[1] & 0xFF) << 8) | (data[0] & 0xFF);
        }
        return this->area;
    }

    void merge_area(int _area) {
        if (has_merged) {
            return;
        }
        if (size <= 0) return;
        //now should check is alloc by me. I have 2 extra head.
        if (m_true_size == size + 2) {
            m_true_data[0] = (_area & 0xFF);
            m_true_data[1] = ((_area >> 8) & 0xFF);
            size = m_true_size;
            data = m_true_data;
        } else {
            //data set by  set_alloced_data or alloc=false,should realloc
            m_true_size = size + 2;
            m_true_data = (char *) malloc(m_true_size);

            //memset(m_true_data, 0, m_true_size+1);
            m_true_data[0] = (_area & 0xFF);
            m_true_data[1] = ((_area >> 8) & 0xFF);

            memcpy(m_true_data + 2, data, size);

            if (alloc) {
                free(data);
                data = NULL;
            }
            alloc = true;
            //reset flags;
            hashcode = 0;
            data = m_true_data;
            size = m_true_size;
        }
        this->area = _area;
        has_merged = true;
    }

    int decode_area() {
        int target_area = area;
        if (has_merged) {
            assert(size > 2);
            target_area = data[1] & 0xFF;
            target_area <<= 8;
            target_area |= data[0] & 0xFF;

            area = target_area;
            has_merged = false;

            data = m_true_data + 2;
            size = m_true_size - 2;
        }
        return target_area;
    }

    void change_area(int new_area) {
        area = new_area;
        if (has_merged) {
            assert(size > 2);
            m_true_data[0] = (new_area & 0xFF);
            m_true_data[1] = ((new_area >> 8) & 0xFF);
        }
    }

    void merge_meta() {
        if (has_meta_merged) return;

        if (size > 0) {
            int new_size = size + sizeof(item_meta_info);
            char *new_data = (char *) malloc(new_size);
            assert(new_data != NULL);

            char *p = new_data;
            memcpy(p, &data_meta, sizeof(item_meta_info));
            p += sizeof(item_meta_info);
            memcpy(p, data, size);

            //log_debug("meta merged, newsize: %d", new_size);
            set_alloced_data(new_data, new_size);
            has_meta_merged = true;
        }

    }

    void decode_meta(bool force_decode = false) {
        if (has_meta_merged || force_decode) {
            assert(size > (int) sizeof(item_meta_info));
            int new_size = size - sizeof(item_meta_info);
            char *new_data = (char *) malloc(new_size);
            assert(new_data != NULL);

            char *p = data;
            memcpy(&data_meta, data, sizeof(item_meta_info));
            p += sizeof(item_meta_info);
            memcpy(new_data, p, new_size);

            set_alloced_data(new_data, new_size);
            has_meta_merged = false;
        }
    }

    void set_alloced_data(const char *data, int size) {
        free_data();
        this->data = this->m_true_data = (char *) data;
        this->size = this->m_true_size = size;
        alloc = true;
    }

    void set_data(const char *new_data, int new_size, bool _need_alloc = true, bool _has_merged = false) {
        free_data();
        if (_need_alloc) {
            if (new_size > 0) {
                m_true_size = _has_merged ? new_size : new_size + 2;//add area
                m_true_data = (char *) malloc(m_true_size);
                assert(m_true_data != NULL);
                this->alloc = true;
                if (_has_merged) {
                    data = m_true_data;
                    size = m_true_size;
                } else {
                    data = m_true_data + 2;
                    size = m_true_size - 2;
                    m_true_data[0] = m_true_data[1] = 0xFF;
                }

                if (new_data) {
                    memcpy(data, new_data, size);
                } else {
                    memset(data, 0, size);
                }
            }
        } else {
            m_true_data = data = (char *) new_data;
            m_true_size = size = new_size;
            has_merged = _has_merged;
        }
        if (has_merged) {
            area = data[1] & 0xFF;
            area <<= 8;
            area |= data[0] & 0xFF;
        }
    }

    inline char *drop_true_data() {
        char *temp = m_true_data;
        m_true_data = NULL;
        alloc = false;
        return temp;
    }

    inline int32_t drop_true_size() {
        int32_t temp = m_true_size;
        m_true_size = 0;
        return temp;
    }

    inline char *get_data() const {
        return data;
    }

    inline char *get_prefix() const {
        if (prefix_size > 0)
            return has_merged ? data + 2 : data;
        else
            return NULL;
    }

    inline int get_size() const {
        return size;
    }

    int get_prefix_size() const {
        return this->prefix_size;
    }

    void set_prefix_size(int prefix_size) {
        this->prefix_size = prefix_size;
        this->data_meta.prefixsize = prefix_size;
    }

    void set_version(uint16_t version) {
        data_meta.version = version;
    }

    uint16_t get_version() const {
        return data_meta.version;
    }

    uint32_t get_hashcode() const {
        if (hashcode == 0 && size > 0 && data != NULL) {
            int32_t diff_size = this->has_merged ? TAIR_AREA_ENCODE_SIZE : 0;
            if (get_prefix_size() == 0) {
                hashcode = string_util::mur_mur_hash(get_data() + diff_size, get_size() - diff_size);
            } else {
                hashcode = string_util::mur_mur_hash(get_data() + diff_size, get_prefix_size());
            }
        }

        return hashcode;
    }

    uint32_t get_cdate() const {
        return data_meta.cdate;
    }

    void set_cdate(uint32_t cdate) {
        data_meta.cdate = cdate;
    }

    uint32_t get_mdate() const {
        return data_meta.mdate;
    }

    void set_mdate(uint32_t mdate) {
        data_meta.mdate = mdate;
    }

    inline bool is_alloc() const {
        return alloc;
    }

    size_t encoded_size() const {
        return 40 + get_size();
    }

    void encode(DataBuffer *output, bool need_compress = false) const;

    void encode_with_compress(DataBuffer *output) const {
        encode(output, false);
    }

    bool decode(DataBuffer *input, bool need_decompress = false);

    bool decode_with_decompress(DataBuffer *input) {
        return decode(input, true);
    }

private:
#ifdef WITH_COMPRESS
    void do_compress(DataBuffer *output) const;
    bool do_decompress();
#endif

    void init() {
        alloc = false;
        size = m_true_size = 0;
        prefix_size = 0;
        data = m_true_data = NULL;

        has_merged = false;
        has_meta_merged = false;
        area = 0;
        hashcode = 0;
        server_flag = 0;
        memset(&data_meta, 0, sizeof(item_meta_info));
    }

    inline void free_data() {
        if (m_true_data && alloc) {
            free(m_true_data);
        }
        data = NULL;
        m_true_data = NULL;
        alloc = false;
        size = m_true_size = 0;
        hashcode = 0;
        has_merged = false;
        area = 0;
    }

private:
    int size;
    int prefix_size;
    char *data;
    bool alloc;
    mutable uint64_t hashcode;

    int m_true_size;  //if has_merged ,or m_true_size=size+2
    char *m_true_data; //if has_merged arae,same as data,or data=m_true_data+2;
public:
    bool has_merged;
    bool has_meta_merged;
    uint32_t area;
    uint16_t server_flag;
    item_meta_info data_meta;
#ifdef WITH_COMPRESS
    static int compress_type;
    static int compress_threshold;
#endif
};

class data_entry_comparator {
public:
    bool operator()(const data_entry *a, const data_entry *b) {
        return ((*a) < (*b));
    }
};

struct data_entry_hash {
    size_t operator()(const data_entry *a) const {
        if (a == NULL || a->get_data() == NULL || a->get_size() == 0) return 0;

        return string_util::mur_mur_hash(a->get_data(), a->get_size());
    }
};

struct data_entry_equal_to {
    bool operator()(const data_entry *a, const data_entry *b) const {
        return *a == *b;
    }
};


// we don't reserve dummy meta when record entry in some condition(remote synchronization, eg.),
// while need some base info to operate it, here is entry tailer.
// prefix_size and mtime matters now.
class entry_tailer {
public:
    entry_tailer() {}

    entry_tailer(const common::data_entry &entry) {
        set(entry);
    }

    entry_tailer(const char *data, int32_t size) {
        set(data, size);
    }

    ~entry_tailer() {
    }

    inline void set(const common::data_entry &entry) {
        tailer_.prefix_size_ = entry.get_prefix_size();
        tailer_.mdate_ = entry.data_meta.mdate > 0 ? entry.data_meta.mdate : time(NULL);
    }

    inline void set(const char *data, int32_t size) {
        UNUSED(size);
        tailer_ = *(reinterpret_cast<tailer *>(const_cast<char *>(data)));
    }

    inline char *data() {
        // just ignore endian now.
        // variant encode may be better
        return reinterpret_cast<char *>(&tailer_);
    }

    inline int32_t size() {
        return sizeof(tailer);
    }

    inline void consume_tailer(common::data_entry &entry) {
        entry.set_prefix_size(tailer_.prefix_size_);
        entry.data_meta.cdate = entry.data_meta.mdate = tailer_.mdate_;
    }

    static bool need_entry_tailer(const common::data_entry &) {
        // return entry.get_prefix_size() > 0;
        // now, mtime need record all the time
        return true;
    }

private:
    typedef struct tailer {
        tailer() : prefix_size_(0), mdate_(0) {}

        uint32_t prefix_size_;
        uint32_t mdate_;
    } tailer;
    tailer tailer_;
};

//! Be Cautious about the return value of set::insert & hash_map::insert,
//! which return pair<iterator, bool> type.
//! When inserting one existing data_entry to a set or hash_map below,
//! there is a risk of memory leaks
typedef std::vector<data_entry *> tair_dataentry_vector;
typedef std::set<data_entry *, data_entry_comparator> tair_dataentry_set;
typedef __gnu_cxx::hash_map<data_entry *, data_entry *,
        data_entry_hash, data_entry_equal_to> tair_keyvalue_map;
typedef __gnu_cxx::hash_map<data_entry *, int, data_entry_hash, data_entry_equal_to> key_code_map_t;

class value_entry {
public:
    value_entry() : version(0), expire(0) {
    }

    value_entry(const value_entry &entry) {
        d_entry = entry.d_entry;
        version = entry.version;
        expire = entry.expire;
    }

    value_entry &clone(const value_entry &entry) {
        assert(this != &entry);
        d_entry = entry.d_entry;
        version = entry.version;
        expire = entry.expire;

        return *this;
    }

    void set_d_entry(const data_entry &in_d_entry) {
        d_entry = in_d_entry;
    }

    data_entry &get_d_entry() {
        return d_entry;
    }

    void set_expire(int32_t expire_time) {
        expire = expire_time;
    }

    int32_t get_expire() const {
        return expire;
    }

    void set_version(uint16_t kv_version) {
        version = kv_version;
    }

    uint16_t get_version() const {
        return version;
    }

    void encode(DataBuffer *output) const;

    bool decode(DataBuffer *input);

    size_t encoded_size() const {
        return d_entry.encoded_size() + sizeof(version) + sizeof(expire);
    }

    int get_size() const {
        return d_entry.get_size() + 2 + 4;
    }

private:
    data_entry d_entry;
    uint16_t version;
    int32_t expire;
};

class mput_record {
public:
    mput_record() {
        key = NULL;
        value = NULL;
    }

    mput_record(mput_record &rec) {
        key = new data_entry(*(rec.key));
        value = new value_entry(*(rec.value));
    }

    ~mput_record() {
        if (key != NULL) {
            delete key;
            key = NULL;
        }
        if (value != NULL) {
            delete value;
            value = NULL;
        }
    }

public:
    data_entry *key;
    value_entry *value;
};

typedef std::vector<mput_record *> mput_record_vec;

typedef __gnu_cxx::hash_map<data_entry *, value_entry *, data_entry_hash> tair_client_kv_map;


inline void defree(tair_dataentry_vector &vector) {
    for (size_t i = 0; i < vector.size(); ++i) {
        delete vector[i];
    }
}

inline void defree(tair_dataentry_set &set) {
    tair_dataentry_set::iterator itr = set.begin();
    while (itr != set.end()) {
        delete *itr++;
    }
}

inline void defree(tair_keyvalue_map &map) {
    tair_keyvalue_map::iterator itr = map.begin();
    while (itr != map.end()) {
        data_entry *key = itr->first;
        data_entry *value = itr->second;
        ++itr;
        delete key;
        delete value;
    }
}

inline void defree(key_code_map_t &map) {
    key_code_map_t::iterator itr = map.begin();
    while (itr != map.end()) {
        data_entry *key = itr->first;
        ++itr;
        delete key;
    }
}

inline void merge_key(const data_entry &pkey, const data_entry &skey, data_entry &mkey) {
    int pkey_size = pkey.get_size();
    int skey_size = skey.get_size();
    char *buf = (char *) malloc(pkey_size + skey_size);
    memcpy(buf, pkey.get_data(), pkey_size);
    memcpy(buf + pkey_size, skey.get_data(), skey_size);
    mkey.set_alloced_data(buf, pkey_size + skey_size);
    mkey.set_prefix_size(pkey_size);
}

inline void split_key(const data_entry *mkey, data_entry *pkey, data_entry *skey) {
    if (mkey == NULL)
        return;
    int prefix_size = mkey->get_prefix_size();
    if (pkey != NULL) {
        pkey->set_data(mkey->get_data(), prefix_size);
    }
    if (skey != NULL) {
        skey->set_data(mkey->get_data() + prefix_size, mkey->get_size() - prefix_size);
    }
}
}
}

#endif
