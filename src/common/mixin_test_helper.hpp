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

#ifndef _COMMON_MIXIN_TEST_
#define _COMMON_MIXIN_TEST_

#include <string>
#include <map>

namespace tair {
namespace common {
namespace MixinTest {

typedef enum {
    Unknown,
    Integer,
    Double,
    Pointer,
} ValueType;

class Value {
public:
    Value();

    Value(const Value &value);

    const char *ToString() const;

public:
    ValueType type_;
    union {
        long long intval_;
        double dbval_;
        void *ptrval_;
    } data_;
};

class Mixin {
public:
    Value Get(const char *key);

    void Put(const char *key, Value &value);

private:
    std::map<std::string, Value> map_;
};
}
}
}

#endif
