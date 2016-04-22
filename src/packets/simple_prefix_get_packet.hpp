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

#ifndef SIMPLE_PREFIX_GET_PACKET_HPP
#define SIMPLE_PREFIX_GET_PACKET_HPP

#include <vector>

#include "base_packet.hpp"

namespace tair {

// len == -1 means end
struct slice {
    int16_t len;
    void *buf;

    slice() : len(0), buf(NULL) {}
};

struct key_element {
    // keys[0] primekey keys[1...n] subkey
    slice primekey;
    std::vector<slice> subkeys;
};

struct kv_element {
    // index 0 prime no value
    slice primekey;
    // primeval is compatible for key without prefix,
    // and resize for key with prefix
    slice primeval;
    int16_t flag;

    std::vector<slice> subkeys;
    std::vector<slice> subvals;
    std::vector<int16_t> substatus;

    kv_element() : flag(0) {}

    size_t subsize() {
        assert(subkeys.size() == subvals.size());
        assert(subkeys.size() == substatus.size());
        return subkeys.size();
    }

    void subresize(int32_t size) {
        subkeys.resize(size);
        subvals.resize(size);
        substatus.resize(size);
    }
};

static void free_slice(slice &s) {
    if (s.buf == NULL)
        return;
    ::free(s.buf);
    s.buf = NULL;
}

static bool read_slice_append_prime(DataBuffer *input, uint16_t area, const slice &prime, slice &sub, int32_t &total) {
    if (total < 2)
        return false;
    int16_t slen = input->readInt16();
    total -= 2;
    // key or value less than 16k
    if (slen > (1024 * 8) || slen < 0) {
        log_warn("slice too large %d bytes", slen);
        return false;
    }
    if (slen == 0) {
        sub.len = 0;
        sub.buf = NULL;
        return true;
    }
    // avoid memcopy, 2 bytes save area
    sub.len = slen + prime.len + 2;
    sub.buf = malloc(sub.len);
    char *cbuf = (char *) sub.buf;
    cbuf[0] = area & 0xFF;
    cbuf[1] = (area >> 8) & 0xFF;
    memcpy(cbuf + 2, prime.buf, prime.len);
    if (input->readBytes(cbuf + 2 + prime.len, slen)) {
        total -= slen;
        return true;
    } else {
        ::free(sub.buf);
        sub.buf = NULL;
        sub.len = 0;
        return false;
    }
}

static bool read_slice(DataBuffer *input, slice &s, int32_t &total) {
    if (total < 2)
        return false;
    s.len = input->readInt16();
    total -= 2;
    // key or value less than 16k
    if (s.len > (1024 * 8) || s.len < 0) {
        log_warn("slice too large %d bytes", s.len);
        return false;
    }
    if (s.len == 0) {
        s.buf = NULL;
        return true;
    }
    s.buf = (char *) malloc(s.len);
    if (input->readBytes(s.buf, s.len)) {
        total -= s.len;
        return true;
    } else {
        ::free(s.buf);
        return false;
    }
}

static bool write_slice(DataBuffer *output, const slice &s, size_t skip) {
    if (s.len < 0)
        return false;
    output->writeInt16(s.len - skip);
    if (s.len > 0)
        output->writeBytes((char *) (s.buf) + skip, s.len - skip);
    return true;
}

class request_simple_get : public base_packet {
public:

    request_simple_get() : ns_(0) {
        setPCode(TAIR_REQ_SIMPLE_GET_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_READ;
    }

    void set_ns(int ns) { this->ns_ = ns; }

    virtual uint16_t ns() const { return ns_; }

    virtual ~request_simple_get() {
        for (size_t i = 0; i < keyset_.size(); ++i) {
            free_slice(keyset_[i].primekey);
            for (size_t j = 0; j < keyset_[i].subkeys.size(); j++) {
                free_slice(keyset_[i].subkeys[j]);
            }
            keyset_[i].subkeys.clear();
        }
        keyset_.clear();
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16; // header 16 bytes

        size_t total = 16; // net header size
        total += 10;
        total += 2;
        for (size_t i = 0; i < keyset_.size(); ++i) {
            total += keyset_[i].primekey.len + 2;
            total += 2;
            for (size_t j = 0; j < keyset_[i].subkeys.size(); ++j) {
                total += keyset_[i].subkeys[j].len + 2;
            }
        }
        return total;
    }

    /**
     * NS[2] COUNT_OF_KEYS[2]
     * [
     *    LENGTH_OF_PRIMEKEY[2], PRIME_KEY
     *    COUNT_OF_SUBKEYS[2]
     *    [
     *       LENGTH_OF_SUBKEYSIZE[2]
     *       SUB_KEY
     *    ]...
     * ]...
     * size = 2 + 2 + COUNT_OF_KEYS * (2 + LENGTH_OF_PRIMEKEY +
     *        2 + COUNT_OF_SUBKEY * (2 + LENGTH_OF_SUBKEYSIZE))
     */
    virtual bool decode(DataBuffer *input, PacketHeader *header) {
        assert(keyset_.size() == 0);
        int total = header->_dataLen;
        if (total < 12) {
            log_warn("packet buffer too few");
            return false;
        }

        total -= 12;
        reserved_ = input->readInt64();           // 8 bytes
        ns_ = input->readInt16();           // 2 bytes
        int16_t keycount = input->readInt16();    // 2 bytes
        if (keycount >= 1024 || keycount < 0) {
            log_warn("prime key too more %d", keycount);
            return false;
        }
        keyset_.resize(keycount);

        bool success = true;
        for (int i = 0; success && i < keycount; ++i) {
            if (total < 2) {
                success = false;
                break;
            }
            // prime key
            if (read_slice(input, keyset_[i].primekey, total) == false) {
                success = false;
                break;
            }
            // sub keys
            total -= 2;
            int16_t subcount = input->readInt16();
            if (subcount >= 1024 || subcount < 0) {
                log_warn("sub key too more %d", subcount);
                return false;
            }
            keyset_[i].subkeys.resize(subcount);
            for (int j = 0; j < subcount; ++j) {

                if (read_slice_append_prime(input, ns_,
                                            keyset_[i].primekey,
                                            keyset_[i].subkeys[j], total) == false) {
                    success = false;
                    break;
                }
            }
        }
        if (total != 0) {
            log_warn("packet buffer error remain %d bytes", total);
            success = false;
        }
        return success;
    }

    virtual bool encode(DataBuffer *output) {
        log_warn("not support %s", __FUNCTION__);
        return false;
    }

    std::vector<key_element> &mutable_keyset() {
        return keyset_;
    }

private:
    int64_t reserved_;
    uint16_t ns_;
    std::vector<key_element> keyset_;
private:
    request_simple_get(const request_simple_get &);
};

class response_simple_get : public base_packet {
public:

    response_simple_get() : config_version_(0), error_code_(0) {
        setPCode(TAIR_RESP_SIMPLE_GET_PACKET);
    }

    virtual ~response_simple_get() { clear(); }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    virtual bool decode(DataBuffer *input, PacketHeader *header) {
        log_warn("not support %s", __FUNCTION__);
        return false;
    }

    /**
     * CS_VERSION[4] NS[2] COUNT_OF_KEYS[2]
     * [
     *    FLAG
     *    LENGTH_OF_PRIMEKEY[2], PRIME_KEY
     *    COUNT_OF_SUBKEYS[2]
     *    [
     *       FLAG
     *       LENGTH_OF_SUBKEYSIZE[2]
     *       SUB_KEY
     *       LENGTH_OF_VALUE[2]
     *       VALUE
     *    ]...
     * ]...
     * size = 2 + 2 + COUNT_OF_KEYS * (2 + LENGTH_OF_PRIMEKEY +
     *        2 + COUNT_OF_SUBKEY * (2 + LENGTH_OF_SUBKEYSIZE))
     */
    virtual bool encode(DataBuffer *output) {
        output->writeInt32(config_version_);
        output->writeInt16(error_code_);
        output->writeInt16(kvset_.size());
        for (size_t i = 0; i < kvset_.size(); ++i) {
            kv_element &elem = kvset_[i];
            assert(elem.subkeys.size() == elem.subvals.size());
            assert(elem.subkeys.size() == elem.substatus.size());

            output->writeInt16(elem.flag);
            write_slice(output, elem.primekey, 0);
            write_slice(output, elem.primeval, 0);

            output->writeInt16(elem.subkeys.size());
            for (size_t j = 0; j < elem.subkeys.size(); ++j) {
                // prime and sub... key, with value
                output->writeInt16(elem.substatus[j]);
                // skip header + primekey
                if (elem.subkeys[j].len <= elem.primekey.len + 2)
                    return false;
                write_slice(output, elem.subkeys[j], elem.primekey.len + 2);
                write_slice(output, elem.subvals[j], 0);
            }
        }
        return true;
    }

    virtual size_t size() const {
        if (UNLIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 16; // net header size
        total += 4 + 2;
        total += 2;
        for (size_t i = 0; i < kvset_.size(); ++i) {
            total += 2
                     + kvset_[i].primekey.len + 2
                     + kvset_[i].primeval.len + 2;
            total += 2;
            for (size_t j = 0; j < kvset_[i].subkeys.size(); ++j) {
                total += 2
                         + kvset_[i].subkeys[j].len + 2
                         + kvset_[i].subvals[j].len + 2;
            }
        }
        return total;
    }

    void set_config_version(int32_t version) {
        this->config_version_ = version;
    }

    int16_t error_code() {
        return this->error_code_;
    }

    void set_error_code(int16_t err) {
        clear();
        this->error_code_ = err;
    }

    virtual void set_code(int code) {
        this->error_code_ = code;
    }

    void clear() {
        for (size_t i = 0; i < kvset_.size(); ++i) {
            kv_element &elem = kvset_[i];
            free_slice(elem.primekey);
            free_slice(elem.primeval);
            for (size_t j = 0; j < elem.subsize(); ++j) {
                free_slice(elem.subkeys[j]);
                free_slice(elem.subvals[j]);
            }
            elem.subkeys.clear();
            elem.subvals.clear();
            elem.substatus.clear();
        }
        kvset_.clear();
    }

    std::vector<kv_element> &mutable_kvset() {
        return kvset_;
    }

    virtual bool failed() const {
        return error_code_ != TAIR_RETURN_SUCCESS;
    }

private:
    std::vector<kv_element> kvset_;
    uint32_t config_version_;
    int16_t error_code_;
private:
    response_simple_get(const response_simple_get &);
};


} // end namespace tair

#endif
