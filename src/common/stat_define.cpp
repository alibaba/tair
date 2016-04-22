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

#include <unistd.h>
#include <sys/mman.h>
#include <byteswap.h>

#include "stat_define.hpp"

namespace tair {
namespace common {
using std::vector;
using std::string;

//////////////////// StatDesc
//////////////////// StatSchema
StatSchema::StatSchema() {
}

StatSchema::StatSchema(const StatDesc *all_stat_descs, int32_t size, int32_t skip_type,
                       const StatDesc *key_desc, char byteorder) {
    byteorder_ = byteorder;

    const StatDesc *stat_descs = all_stat_descs;
    // skip type
    if (skip_type > 0 && size > 0) {
        StatDesc *new_stat_descs = new StatDesc[size];
        for (int32_t i = 0; i < size; ++i) {
            new_stat_descs[i] = all_stat_descs[i];
            new_stat_descs[i].type_ -= skip_type;
        }
        stat_descs = new_stat_descs;
    }

    int32_t max_type = -1;
    for (int32_t i = 0; i < size; ++i) {
        if (stat_descs[i].type_ > max_type) {
            max_type = stat_descs[i].type_;
        }
    }

    // be compatible to nonconsecutive type in stat_descs,
    // we just locate each stat by its specified type.
    max_type += 2;               // including key
    sizeofs_ = std::vector<int32_t>(max_type, 0);
    descs_ = std::vector<std::string>(max_type, "");
    funcs_ = std::vector<Func>(max_type, Func());

    for (int32_t i = 0; i < size; ++i) {
        const StatDesc &sd = stat_descs[i];
        sizeofs_[sd.type_] = sd.sizeof_;
        descs_[sd.type_] = sd.desc_;
    }
    // key information, in the end
    sizeofs_[sizeofs_.size() - 1] = key_desc->sizeof_;
    descs_[descs_.size() - 1] = key_desc->desc_;

    // get funcs
    int32_t offset = 0;
    for (size_t i = 0; i < sizeofs_.size(); ++i) {
        int32_t sz = sizeofs_[i];
        switch (sz) {
            case 0:
                // hole type, skip
                break;
            case 1:
                funcs_[i] = Func(offset, encode_fixed8, decode_fixed8, reset_fixed8, update_fixed8);
                break;
            case 2:
                funcs_[i] = Func(offset, encode_fixed16, decode_fixed16, reset_fixed16, update_fixed16);
                break;
            case 4:
                funcs_[i] = Func(offset, encode_fixed32, decode_fixed32, reset_fixed32, update_fixed32);
                break;
            case 8:
                funcs_[i] = Func(offset, encode_fixed64, decode_fixed64, reset_fixed64, update_fixed64);
                break;
            default:
                log_error("schema unsupported sizeof: %d", sz);
                break;
        }
        offset += sz;
    }

    serialize_schema();

    if (stat_descs != all_stat_descs) {
        delete[] stat_descs;
    }
}

StatSchema::~StatSchema() {
}

template<typename T>
void trim_tail_and_merge(T &a, const T &b) {
    typename T::iterator it = a.end();
    if (it != a.begin()) {
        it = a.erase(--it);
    }
    a.insert(it, b.begin(), b.end());
}

void StatSchema::merge(const StatSchema &schema) {
    byteorder_ = schema.byteorder_;
    trim_tail_and_merge(sizeofs_, schema.sizeofs_);
    trim_tail_and_merge(descs_, schema.descs_);
    // we need recheck funcs_.offset_
    if (!funcs_.empty()) {
        int32_t base_offset = funcs_[funcs_.size() - 2].offset_ + sizeofs_[sizeofs_.size() - 2];
        std::vector<Func> funcs = schema.funcs_;
        for (size_t i = 0; i < funcs.size(); ++i) {
            funcs[i].offset_ += base_offset;
        }
        trim_tail_and_merge(funcs_, funcs);
    } else {
        trim_tail_and_merge(funcs_, schema.funcs_);
    }

    serialize_schema();
}

int32_t StatSchema::offset(int32_t type) const {
    return funcs_[type].offset_;
}

int32_t StatSchema::stat_type_size() const {
    return descs_.size() - 1;
}

int32_t StatSchema::one_row_stat_size() const {
    return one_row_stat_size_;
}

int32_t StatSchema::key_size() const {
    // last column is key
    return sizeofs_[sizeofs_.size() - 1];
}

int32_t StatSchema::one_row_size() const {
    return one_row_size_;
}

int32_t StatSchema::max_serialized_size(int32_t size) const {
    return one_row_size_ * size;
}

const std::string &StatSchema::get_placeholder() const {
    return placeholder_;
}

const std::string &StatSchema::to_string() const {
    return schema_str_;
}

void StatSchema::serialize_schema() {
    column_size_ = descs_.size();  // including key
    one_row_size_ = 0;

    // to string
    // format:
    // [byteorder][column_count][desc0_size][desc0][desc1_size][desc1]...[column0_sizeof][column1_sizeof]...

    // [byteorder]
    std::string &str = schema_str_;
    str.clear();
    str += byteorder_;

    char buf[32];
    int32_t size = 0;
    // [column_count]
    size = column_size_;
    StatSchema::encode_fixed32(buf, &size);
    str.append(buf, sizeof(int32_t));

    // [desc0_size][desc0][desc1_size][desc1]...
    // first key
    size = descs_[descs_.size() - 1].size();
    StatSchema::encode_fixed32(buf, &size);
    str.append(buf, sizeof(int32_t));
    str.append(descs_[descs_.size() - 1]);
    // other columns
    for (size_t i = 0; i < descs_.size() - 1; ++i) {
        if (sizeofs_[i] > 0)    // not hole type
        {
            size = descs_[i].size();
            StatSchema::encode_fixed32(buf, &size);
            str.append(buf, sizeof(int32_t));
            str.append(descs_[i]);
        }
    }

    // [column0_sizeof][column1_sizeof]...
    // first key
    size = sizeofs_[sizeofs_.size() - 1];
    StatSchema::encode_fixed32(buf, &size);
    str.append(buf, sizeof(int32_t));
    one_row_size_ = size;
    // other columns
    for (size_t i = 0; i < sizeofs_.size() - 1; ++i) {
        if (sizeofs_[i] > 0)    // not hole type
        {
            size = sizeofs_[i];
            StatSchema::encode_fixed32(buf, &size);
            str.append(buf, sizeof(int32_t));
            one_row_size_ += size;
        }
    }

    one_row_stat_size_ = one_row_size_ - key_size();
    placeholder_ = std::string(one_row_stat_size_, '\0');
}

// this is a little wierd
int32_t StatSchema::serialize_one_row_stat(char *buf, int32_t key, Stat *stats) const {
    // key
    int32_t size = funcs_[funcs_.size() - 1].encode_(buf, &key);
    // stat column
    // actually it should be like:
    //   for (int i = 0; i < funcs_.size() - 1; ++i)
    //   {
    //     size += funcs_[i].encode_(buf + size, stats + funcs_[i].offset_);
    //   }
    // Now, stats memory format is just as same as serialized format.
    memcpy(buf + size, stats, one_row_stat_size_);
    return one_row_size_;
}

void StatSchema::set_value(Stat *s, int32_t type, int64_t value) const {
    const Func &func = funcs_[type];
    func.encode_(s + func.offset_, &value);
}

int64_t StatSchema::get_value(Stat *s, int32_t type) const {
    const Func &func = funcs_[type];
    int64_t value = 0;
    func.decode_(s + func.offset_, &value);
    return value;
}

void StatSchema::update(Stat *s, int32_t type, int32_t value) const {
    const Func &func = funcs_[type];
    func.update_(s + func.offset_, value);
}

void StatSchema::update(Stat *base, const Stat *diff, int32_t type) const {
    const Func &func = funcs_[type];
    int64_t value = 0;
    func.decode_(diff + func.offset_, &value);
    func.update_(base + func.offset_, value);
}

void StatSchema::update(Stat *base, const Stat *diff, const std::vector<int32_t> *types) const {
    if (types == NULL || types->empty()) {
        size_t size = funcs_.size() - 1;
        for (size_t i = 0; i < size; ++i) {
            update(base, diff, i);
        }
    } else {
        for (size_t i = 0; i < types->size(); ++i) {
            update(base, diff, (*types)[i]);
        }
    }
}

void StatSchema::reset(Stat *s, int32_t score, int32_t type) const {
    int64_t value = 0, new_value = 0;
    const Func &func = funcs_[type];
    if (0 == score) {
        func.reset_(s + func.offset_, &value, &new_value);
    } else {
        func.decode_(s + func.offset_, &value);
        new_value = value / score;
        if (0 != value && 0 == new_value)
            new_value = 1;
        func.reset_(s + func.offset_, &value, &new_value);
    }
}

void StatSchema::reset(Stat *s, int32_t score, const std::vector<int32_t> *types) const {
    if (types == NULL || types->empty()) {
        size_t size = funcs_.size() - 1;
        for (size_t i = 0; i < size; ++i) {
            reset(s, score, i);
        }
    } else {
        for (size_t i = 0; i < types->size(); ++i) {
            reset(s, score, (*types)[i]);
        }
    }
}

int StatSchema::cmp_stat(const char *a, const char *b) const {
    // compare key
    int64_t key1 = 0, key2 = 0;
    funcs_[funcs_.size() - 1].decode_(a, &key1);
    funcs_[funcs_.size() - 1].decode_(b, &key2);
    return key1 - key2;
}

void StatSchema::merge_stats(int32_t max_row,
                             const StatSchema &schema_a, const char *stat_buf_a, int32_t stat_size_a,
                             const StatSchema &schema_b, const char *stat_buf_b, int32_t stat_size_b,
                             const StatSchema &schema, char *&stat_buf, int32_t &stat_buf_size,
                             int32_t &stat_data_size) {
    int32_t one_row_size_a = schema_a.one_row_size();
    int32_t one_row_size_b = schema_b.one_row_size();
    const std::string &holder_a = schema_a.get_placeholder();
    const std::string &holder_b = schema_b.get_placeholder();
    int32_t max_holder_size = (holder_a.size() + holder_b.size()) * max_row;
    int32_t max_buf_size = stat_size_a + stat_size_b + max_holder_size;

    /*
    // @@@@@@@@@@@@@@@
    StatSchema::debug_stat(schema_a, stat_buf_a, stat_size_a);
    StatSchema::debug_stat(schema_b, stat_buf_b, stat_size_b);
    // @@@@@@@@@@@@@@@
    */

    // realloc stat data buffer
    if (UNLIKELY(max_buf_size > stat_buf_size)) {
        if (stat_buf != NULL) {
            delete[] stat_buf;
            stat_buf = NULL;
        }
        stat_buf_size = max_buf_size;
        stat_buf = new char[stat_buf_size];
    }

    char *pos = stat_buf;

    const char *pos_a = stat_buf_a, *end_a = stat_buf_a + stat_size_a,
            *pos_b = stat_buf_b, *end_b = stat_buf_b + stat_size_b;

    int32_t stat_key_size = schema.key_size();
    int cmp = 0;
    while (pos_a < end_a || pos_b < end_b) {
        if (pos_a >= end_a) {
            cmp = 1;
        } else if (pos_b >= end_b) {
            cmp = -1;
        } else {
            cmp = schema.cmp_stat(pos_a, pos_b);
        }

        if (cmp < 0) {
            // stat_a
            memcpy(pos, pos_a, one_row_size_a);
            // holder_b
            memcpy(pos + one_row_size_a, holder_b.c_str(), holder_b.size());

            pos_a += one_row_size_a;
            pos += one_row_size_a + holder_b.size();
        } else if (cmp > 0) {
            // key
            memcpy(pos, pos_b, stat_key_size);
            // holder_a
            memcpy(pos + stat_key_size, holder_a.c_str(), holder_a.size());
            // stat_b (skip key)
            memcpy(pos + one_row_size_a, pos_b + stat_key_size, one_row_size_b - stat_key_size);

            pos_b += one_row_size_b;
            pos += one_row_size_b + holder_a.size();
        } else {
            // stat_a
            memcpy(pos, pos_a, one_row_size_a);
            // stat_b (skip key)
            memcpy(pos + one_row_size_a, pos_b + stat_key_size, one_row_size_b - stat_key_size);

            pos_a += one_row_size_a;
            pos_b += one_row_size_b;
            pos += one_row_size_a + one_row_size_b - stat_key_size;
        }
    }

    stat_data_size = pos - stat_buf;
    /*
    //@@@@@@@@
    StatSchema::debug_stat(schema, stat_buf, stat_data_size);
    //@@@@@@@@
    */
}

/*
// @@@@@@@@@@@@@@@@
void StatSchema::debug_stat(const StatSchema& schema, const char* stat_buf, int32_t stat_size)
{
  const char* buf = stat_buf;
  fprintf(stderr, "@@@ =========== start ========\n");
  fprintf(stderr, "byteorder: %c", *buf);
  buf++;
  int32_t column = *(int32_t*)buf;
  int32_t size = 0;
  buf+=4;
  fprintf(stderr, "\n@@ %d desc: ", column);
  for (int i = 0; i < column; ++i)
  {
    size = *(int32_t*)buf;
    buf += 4;
    fprintf(stderr, "[%s]", std::string(buf, size).c_str());
    buf += size;
  }
  fprintf(stderr, "\n@@ sizeof: ");
  for (int i = 0; i < column; ++i)
  {
    size = *(int32_t*)buf;
    buf += 4;
    fprintf(stderr, "[%d]", size);
  }

  fprintf(stderr, "\n@@ stat: ");
  const Func* func = NULL;
  int64_t value = 0;
  while (buf < stat_buf + stat_size)
  {
    // key
    value = 0;
    buf += schema.funcs_[schema.funcs_.size() - 1].decode_(buf, &value);
    fprintf(stderr, "\n%ld ", value);
    // other
    for (size_t i = 0; i < schema.funcs_.size() - 1; ++i)
    {
      func = &schema.funcs_[i];
      value = 0;
      buf += func->decode_(buf, &value);
      fprintf(stderr, "%ld ", value);
    }
  }
  fprintf(stderr, "\n@@@ %p %p %d %p=========== end ========\n", buf, stat_buf, stat_size, stat_buf + stat_size);
}
*/

// Now, we just copy raw data whose byteorder is based on architecture itself.
int32_t StatSchema::encode_fixed8(char *pos, const void *value) {
    *reinterpret_cast<int8_t *>(pos) = *reinterpret_cast<const int8_t *>(value);
    return sizeof(int8_t);
}

int32_t StatSchema::decode_fixed8(const char *pos, void *value) {
    *reinterpret_cast<int8_t *>(value) = *reinterpret_cast<const int8_t *>(pos);
    return sizeof(int8_t);
}

int32_t StatSchema::reset_fixed8(char *pos, void *value, const void *new_value) {
    *reinterpret_cast<int8_t *>(value) =
            tair::common::atomic_exchange(reinterpret_cast<uint8_t *>(pos),
                                          *reinterpret_cast<const uint8_t *>(new_value));
    return sizeof(int8_t);
}

void StatSchema::update_fixed8(void *base, int64_t value) {
    tair::common::atomic_add(reinterpret_cast<uint8_t *>(base), static_cast<uint8_t>(value));
}

int32_t StatSchema::encode_fixed16(char *pos, const void *value) {
    *reinterpret_cast<int16_t *>(pos) = *reinterpret_cast<const int16_t *>(value);
    return sizeof(int16_t);
}

int32_t StatSchema::decode_fixed16(const char *pos, void *value) {
    *reinterpret_cast<int16_t *>(value) = *reinterpret_cast<const int16_t *>(pos);
    return sizeof(int16_t);
}

int32_t StatSchema::reset_fixed16(char *pos, void *value, const void *new_value) {
    *reinterpret_cast<int16_t *>(value) =
            tair::common::atomic_exchange(reinterpret_cast<uint16_t *>(pos),
                                          *reinterpret_cast<const uint16_t *>(new_value));
    return sizeof(int16_t);
}

void StatSchema::update_fixed16(void *base, int64_t value) {
    tair::common::atomic_add(reinterpret_cast<uint16_t *>(base), static_cast<uint16_t>(value));
}

int32_t StatSchema::encode_fixed32(char *pos, const void *value) {
    *reinterpret_cast<int32_t *>(pos) = *reinterpret_cast<const int32_t *>(value);
    return sizeof(int32_t);
}

int32_t StatSchema::decode_fixed32(const char *pos, void *value) {
    *reinterpret_cast<int32_t *>(value) = *reinterpret_cast<const int32_t *>(pos);
    return sizeof(int32_t);
}

int32_t StatSchema::reset_fixed32(char *pos, void *value, const void *new_value) {
    *reinterpret_cast<int32_t *>(value) =
            tair::common::atomic_exchange(reinterpret_cast<uint32_t *>(pos),
                                          *reinterpret_cast<const uint32_t *>(new_value));
    return sizeof(int32_t);
}

void StatSchema::update_fixed32(void *base, int64_t value) {
    tair::common::atomic_add(reinterpret_cast<uint32_t *>(base), static_cast<uint32_t>(value));
}

int32_t StatSchema::encode_fixed64(char *pos, const void *value) {
    *reinterpret_cast<int64_t *>(pos) = *reinterpret_cast<const int64_t *>(value);
    return sizeof(int64_t);
}

int32_t StatSchema::decode_fixed64(const char *pos, void *value) {
    *reinterpret_cast<int64_t *>(value) = *reinterpret_cast<const int64_t *>(pos);
    return sizeof(int64_t);
}

int32_t StatSchema::reset_fixed64(char *pos, void *value, const void *new_value) {
    *reinterpret_cast<int64_t *>(value) =
            tair::common::atomic_exchange(reinterpret_cast<uint64_t *>(pos),
                                          *reinterpret_cast<const uint64_t *>(new_value));
    return sizeof(int64_t);
}

void StatSchema::update_fixed64(void *base, int64_t value) {
    tair::common::atomic_add(reinterpret_cast<uint64_t *>(base), static_cast<uint64_t>(value));
}

DeStatSchema::DeStatSchema() : one_row_size_(0), version_(0), decode_func_(NULL) {
}

DeStatSchema::~DeStatSchema() {
}

bool DeStatSchema::init(const char *schema_string, int32_t len, int32_t version) {
    if (len < 5)
        return false;

#if __BYTE_ORDER == __LITTLE_ENDIAN
    static const char machine_byteorder = '0';
#else
    static const char machine_byteorder = '1';
#endif

    const char *pos = schema_string;
    //get byteorder
    char packet_byteorder = *pos++;
    decode_func_ = (packet_byteorder == machine_byteorder) ? &decode : &decode_reverse;

    //get number of columns
    int32_t column_size = (int32_t) decode_func_(pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    //get column descs_, [desc0_size][desc0][desc1_size][desc1]...
    for (int32_t i = 0; i < column_size; ++i) {
        int32_t len = (int32_t) decode_func_(pos, sizeof(int32_t));
        pos += sizeof(int32_t);
        descs_.push_back(string(pos, len));
        pos += len;
    }

    // get column_sizeof
    one_row_size_ = 0;
    for (int32_t i = 0; i < column_size; ++i) {
        int32_t len = (int32_t) decode_func_(pos, sizeof(int32_t));
        pos += sizeof(int32_t);
        sizeofs_.push_back(len);
        one_row_size_ += len;
    }

    version_ = version;
    return true;
}

void DeStatSchema::clear() {
    sizeofs_.clear();
    descs_.clear();
    one_row_size_ = 0;
    version_ = 0;
    decode_func_ = NULL;
}

bool DeStatSchema::deserialize(const char *buf, int32_t buf_len, vector<int64_t> &value) {
    if (UNLIKELY(0 == one_row_size_)) {
        log_error("stat schema not init");
        return false;
    }

    // simple check.
    if (UNLIKELY(buf_len % one_row_size_ != 0)) {
        log_error("stat data dismatch with stat schema");
        return false;
    }

    const char *pos = buf;
    const char *buf_end = buf + buf_len;
    size_t column_size = descs_.size();
    // get every row value
    while (pos < buf_end) {
        for (size_t i = 0; i < column_size; ++i) {
            int64_t data = decode_func_(pos, sizeofs_[i]);
            pos += sizeofs_[i];
            value.push_back(data);
        }
    }

    return true;
}

int64_t DeStatSchema::decode(const char *pos, int32_t size) {
    switch (size) {
        case 1:
            return *reinterpret_cast<const int8_t *>(pos);
        case 2:
            return *reinterpret_cast<const int16_t *>(pos);
        case 4:
            return *reinterpret_cast<const int32_t *>(pos);
        case 8:
            return *reinterpret_cast<const int64_t *>(pos);
        default:
            return 0;
    }
}

int64_t DeStatSchema::decode_reverse(const char *pos, int32_t size) {
    switch (size) {
        case 1:
            return *reinterpret_cast<const int8_t *>(pos);
        case 2:
            return bswap_16(*reinterpret_cast<const int16_t *>(pos));
        case 4:
            return bswap_32(*reinterpret_cast<const int32_t *>(pos));
        case 8:
            return bswap_64(*reinterpret_cast<const int64_t *>(pos));
        default:
            return 0;
    }
}

//////////////////// MemStatStore
MemStatStore::MemStatStore(const StatSchema *schema) {
    schema_ = schema;
}

MemStatStore::~MemStatStore() {
}

StatStore *MemStatStore::clone(int32_t index) {
    UNUSED(index);
    return new MemStatStore(schema_);
}

int MemStatStore::start() {
    return TAIR_RETURN_SUCCESS;
}

int MemStatStore::stop() {
    return TAIR_RETURN_SUCCESS;
}

int MemStatStore::destroy() {
    return TAIR_RETURN_SUCCESS;
}

Stat *MemStatStore::add_stat(int32_t key) {
    UNUSED(key);
    int32_t size = schema_->one_row_stat_size();
    Stat *s = new char[size];
    memset(s, 0, size);
    return s;
}

void MemStatStore::clear_stat(Stat *s, bool drop) {
    UNUSED(drop);
    LAZY_DESTROY(s, (any_cleanup<Stat *>));
}

//////////////////// DiskStatStore
DiskStatStore::DiskStatStore(const StatSchema *schema, int32_t version, int32_t max_unit_count, const char *path)
        : version_(version), max_unit_count_(max_unit_count) {
    schema_ = schema;

    unit_size_ = Unit::byte_size(schema_->one_row_stat_size());
    unit_start_ = NULL;

    snprintf(path_, sizeof(path_), "%s", path);
    mmap_data_ = NULL;
    mmap_size_ = 0;
    bitmap_ = NULL;

    iter_ = 0;
}

DiskStatStore::~DiskStatStore() {
    stop();
}

StatStore *DiskStatStore::clone(int32_t index) {
    char new_path[TAIR_MAX_PATH_LEN];
    // append index to path
    snprintf(new_path, sizeof(new_path), "%s.%06d", path_, index);
    return new DiskStatStore(schema_, version_, get_header()->max_unit_count_, new_path);
}

int DiskStatStore::start() {
    return build_stat();
}

int DiskStatStore::stop() {
    int ret = TAIR_RETURN_SUCCESS;

    if (mmap_data_ != NULL) {
        if ((ret = msync(mmap_data_, mmap_size_, MS_SYNC)) != 0) {
            log_error("msync data fail, error: %s", strerror(errno));
        } else if ((ret = munmap(mmap_data_, mmap_size_)) != 0) {
            log_error("munmap data fail, error: %s", strerror(errno));
        } else {
            mmap_data_ = NULL;
            mmap_size_ = 0;
            if (bitmap_ != NULL) {
                delete bitmap_;
                bitmap_ = NULL;
            }
        }
    }

    return ret == 0 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
}

int DiskStatStore::destroy() {
    // remove stat file
    if (::remove(path_) != 0) {
        log_error("remove file fail: %s, error: %s", path_, strerror(errno));
    }
    return TAIR_RETURN_SUCCESS;
}

Stat *DiskStatStore::add_stat(int32_t key) {
    Stat *s = NULL;
    int32_t index = bitmap_->pick();
    if (index >= 0) {
        Unit *su = get_unit(index);
        s = su->stat_;
        memset(s, 0, schema_->one_row_stat_size());
        su->key_ = key;
        bitmap_->set(index);

        flush();
    } else {
        // no space left, so rebuild
        log_error("stat file %s, no space left", path_);
    }

    return s;
}

void DiskStatStore::clear_stat(Stat *s, bool drop) {
    // only reset when drop
    if (drop) {
        bitmap_->reset(get_unit_index(Unit::stat_to_unit(s)));
        flush();
    }
}

void DiskStatStore::seek_stat() {
    iter_ = 0;
}

bool DiskStatStore::next_stat(int32_t &key, Stat *&s) {
    s = NULL;
    int32_t count = get_header()->max_unit_count_;
    for (int32_t i = iter_; i < count; ++i) {
        if (bitmap_->test(i)) {
            Unit *u = get_unit(i);
            key = (int32_t) u->key_;
            s = u->stat_;
            iter_ = i + 1;
            break;
        }
    }
    return s != NULL;
}

int DiskStatStore::build_stat() {
    int fd = open(path_, O_RDWR | O_CREAT | O_LARGEFILE, 0600);
    if (UNLIKELY(fd < 0)) {
        log_error("open stat file fail, name: %s, error: %s", path_, strerror(errno));
        return TAIR_RETURN_FAILED;
    }

    int ret = build_stat_from_exist(fd);
    if (ret != TAIR_RETURN_SUCCESS)
        ret = build_stat_from_new(fd);

    close(fd);
    return ret;
}

int DiskStatStore::build_stat_from_exist(int fd) {
    struct stat s;
    if (UNLIKELY(fstat(fd, &s) != 0)) {
        log_error("get file size fail, name: %s, error: %s", path_, strerror(errno));
        return TAIR_RETURN_FAILED;
    }

    int64_t size = s.st_size;
    if (size < (int64_t) sizeof(Header))
        return TAIR_RETURN_FAILED;

    void *data = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        log_error("map file fail. name: %s, size: %"PRI64_PREFIX"d, error: %s",
                  path_, size, strerror(errno));
        return TAIR_RETURN_FAILED;
    }

    mmap_data_ = reinterpret_cast<char *>(data);
    Header *header = get_header();
    if (header->version_ == version_) {
        mmap_size_ = size;
        bitmap_ = new tair::util::BitMap(get_bitmap_data(), header->max_unit_count_);
        int64_t unit_offset = (sizeof(Header) + bitmap_->byte_size() + 8 - 1) & (~(8 - 1));
        unit_start_ = reinterpret_cast<Unit *>(mmap_data_ + unit_offset);
        //in this case, max_unit_count_ no use
        log_warn("build stat from original stat file, %s", path_);
        return TAIR_RETURN_SUCCESS;
    }

    // stat file is not compatible
    if (munmap(data, size) != 0)
        log_error("munmap file fail. name: %s, size: %"PRI64_PREFIX"d, error: %s", path_, size, strerror(errno));

    return TAIR_RETURN_FAILED;
}

int DiskStatStore::build_stat_from_new(int fd) {
    int64_t size = sizeof(Header) /* header */ + tair::util::BitMap::byte_size(max_unit_count_) /* bitmap */;
    //up bound to multiple of 8
    size = (size + (8 - 1)) & (~(8 - 1));
    // unit begin in unit_offset
    int64_t unit_offset = size;
    // max_unit_count__ is already multiple of 8
    size += unit_size_ * max_unit_count_;

    if (ftruncate(fd, size) < 0) {
        log_error("reserve file size fail, name: %s, size: %"PRI64_PREFIX"d, error: %s",
                  path_, size, strerror(errno));
        return TAIR_RETURN_FAILED;
    }

    void *data = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        log_error("map file fail. name: %s, size: %"PRI64_PREFIX"d, error: %s",
                  path_, size, strerror(errno));
        return TAIR_RETURN_FAILED;
    }
    mmap_data_ = reinterpret_cast<char *>(data);
    mmap_size_ = size;
    unit_start_ = reinterpret_cast<Unit *>(mmap_data_ + unit_offset);
    bitmap_ = new tair::util::BitMap(get_bitmap_data(), max_unit_count_);

    Header *header = get_header();
    header->version_ = version_;
    header->max_unit_count_ = max_unit_count_;

    // clear bitmap key
    memset(get_bitmap_data(), 0, tair::util::BitMap::byte_size(header->max_unit_count_));

    flush();
    log_warn("build stat from new stat file, %s", path_);
    return TAIR_RETURN_SUCCESS;
}

int DiskStatStore::flush(bool meta_only) {
    int64_t size = meta_only ? (sizeof(Header) + bitmap_->byte_size()) : mmap_size_;
    int ret = msync(mmap_data_, size, MS_ASYNC);
    if (ret != 0) {
        log_error("msync data fail, size: %"PRI64_PREFIX"d, error: %s", size, strerror(errno));
    }
    return ret != 0 ? TAIR_RETURN_FAILED : TAIR_RETURN_SUCCESS;
}

DiskStatStore::Header *DiskStatStore::get_header() {
    return reinterpret_cast<Header *>(mmap_data_);
}

char *DiskStatStore::get_bitmap_data() {
    return mmap_data_ + sizeof(Header);
}

DiskStatStore::Unit *DiskStatStore::get_unit(int32_t index) {
    return reinterpret_cast<Unit *>(reinterpret_cast<char *>(unit_start_) + unit_size_ * index);
}

int32_t DiskStatStore::get_unit_index(Unit *su) {
    return (reinterpret_cast<char *>(su) -
            reinterpret_cast<char *>(unit_start_)) / unit_size_;
}

//////////////////// OpStat
// new stat type need ADD ONLY need be added to this array
const StatDesc OpStat::stat_descs_[] =
        {
                {OP_PUT,           4, "put"},
                {OP_PUT_KEY,       4, "put_key"},
                {OP_GET,           4, "get"},
                {OP_GET_KEY,       4, "get_key"},
                {OP_HIT,           4, "hit"},
                {OP_REMOVE_KEY,    4, "remove_key"},
                {OP_HIDE,          4, "hide"},
                {OP_ADD,           4, "add"},
                {OP_PF_PUT,        4, "pf_put"},
                {OP_PF_PUT_KEY,    4, "pf_put_key"},
                {OP_PF_GET,        4, "pf_get"},
                {OP_PF_GET_KEY,    4, "pf_get_key"},
                {OP_PF_HIT,        4, "pf_hit"},
                {OP_PF_REMOVE_KEY, 4, "pf_remove_key"},
                {OP_PF_HIDE,       4, "pf_hide"},
                {OP_GET_RANGE,     4, "get_range"},
                {OP_GET_RANGE_KEY, 4, "get_range_key"},
                {OP_FC_IN,         4, "fc_in"},
                {OP_FC_OUT,        4, "fc_out"},
                {OP_MIGRATE_KEY,   4, "migrate_key"},
                {OP_MIGRATE_SIZE,  4, "migrate_flow_in"}
        };

OpStat::OpStat() :
        schema_(stat_descs_, OP_STAT_TYPE_MAX) {
    stat_ = new StatManager<StatUnit<MemStatStore>, MemStatStore>(new MemStatStore(&schema_));
    last_stat_ = new StatManager<StatUnit<MemStatStore>, MemStatStore>(new MemStatStore(&schema_));
}

OpStat::~OpStat() {
    delete stat_;
    delete last_stat_;
}

void OpStat::add(const StatSchema &extra) {
    schema_.merge(extra);
}

void OpStat::reset(int32_t score) {
    stat_->reset(score);
}

void OpStat::update(int32_t area, int32_t type, int32_t value) {
    // OPT: schema_->update(stat_->get_stat(area), type, value);
    stat_->update(area, type, value);
}

void OpStat::update_or_not(int32_t area, int32_t type, int32_t type_suc, int rc, int32_t value) {
    Stat *s = stat_->get_stat(area);
    if (LIKELY(s != NULL)) {
        schema_.update(s, type, value);
        if (TAIR_RETURN_SUCCESS == rc)
            schema_.update(s, type_suc, value);
    }
}

Stat *OpStat::get_stat(int32_t area) {
    return stat_->get_stat(area);
}

void OpStat::sampling(int32_t interval_s) {
    last_stat_->reset(0);
    StatManager<StatUnit<MemStatStore>, MemStatStore> *tmp = stat_;
    stat_ = last_stat_;
    last_stat_ = tmp;
    last_stat_->reset(interval_s);
}

void OpStat::serialize(char *&buf, int32_t &size, bool &alloc) {
    last_stat_->serialize(buf, size, alloc);
}

const StatSchema *OpStat::get_schema() {
    return &schema_;
}

int32_t OpStat::get_type_size() {
    return schema_.stat_type_size();
}

}
}
