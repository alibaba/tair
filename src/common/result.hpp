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

#ifndef COMMON_RESULT_HPP_
#define COMMON_RESULT_HPP_

namespace tair {
namespace common {
template<class T>
class Result {
public:
    Result(T value, int code) {
        value_ = value;
        code_ = code;
    }

public:
    T value_;
    int code_;
};
}
}

#endif
