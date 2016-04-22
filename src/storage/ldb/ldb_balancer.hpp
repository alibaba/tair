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

#ifndef TAIR_STORAGE_LDB_BALANCER_H
#define TAIR_STORAGE_LDB_BALANCER_H

#include <string>
#include <vector>

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

namespace tbsys {
class CDefaultRunnable;

class CThread;
}


namespace tair {
namespace storage {
namespace ldb {
class BucketIndexer;

class LdbManager;

// balance ldb instance one-time
class LdbBalancer {
public:
    class Balancer : public tbsys::CDefaultRunnable {
    public:
        Balancer(LdbBalancer *owner, LdbManager *manager);

        virtual ~Balancer();

        void run(tbsys::CThread *thread, void *arg);

    private:
        // balance unit:
        //   balance `bucket from `from instance to `to instance
        struct Unit {
            Unit(int32_t bucket, int32_t from, int32_t to) :
                    bucket_(bucket), from_(from), to_(to) {}

            int32_t bucket_;      // bucket
            int32_t from_;        // from instance index
            int32_t to_;          // to instance index
            std::string to_string() const {
                char buf[32];
                snprintf(buf, sizeof(buf), "[bucket %d: %d => %d]",
                         bucket_, from_, to_);
                return std::string(buf);
            }
        };

        static const int32_t MAX_BATCH_SIZE = 512 << 10;                // 512k

    private:
        void try_balance(const BucketIndexer::INDEX_BUCKET_MAP &index_map, std::vector<Unit> &result_units);

        int do_balance(const std::vector<Unit> &units);

        int do_one_balance(const Unit &unit);

    private:
        LdbBalancer *owner_;
        LdbManager *manager_;
    };

public:
    LdbBalancer(LdbManager *manager);

    ~LdbBalancer();

    void start();

    void stop();

    void set_wait_us(int64_t us);

    int64_t get_wait_us();

    void finish(Balancer *balancer);

private:
    LdbManager *manager_;
    Balancer *balancer_;
    int64_t wait_us_;
};

}
}
}
#endif
