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

#include "mixin_test_helper.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace tair {
namespace common {
namespace MixinTest {

Value::Value() {
    type_ = Unknown;
}

Value::Value(const Value &value) {
    this->type_ = value.type_;
    this->data_ = value.data_;
}

const char *Value::ToString() const {
    char *buff = (char *) malloc(sizeof(char) * 64);
    switch (type_) {
        case Integer:
            snprintf(buff, 64, "integer: %lld", data_.intval_);
            break;
        case Double:
            snprintf(buff, 64, "double: %lf", data_.dbval_);
            break;
        case Pointer:
            snprintf(buff, 64, "pointer: 0x%p", data_.ptrval_);
            break;
        default:
            snprintf(buff, 64, "unknown type");
    }
    buff[63] = '\0';
    return buff;
}

Value Mixin::Get(const char *key) {
    std::map<std::string, Value>::iterator iter = map_.find(key);
    if (iter == map_.end()) return Value();
    return iter->second;
}

void Mixin::Put(const char *key, Value &value) {
    map_.insert(std::make_pair<std::string, Value>(key, value));
}
}
}
}
