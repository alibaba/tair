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

#include <tbsys.h>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "leveldb/options.h"
#include "db/dbformat.h"

#include "ldb_manager.hpp"
#include "ldb_instance.hpp"
#include "ldb_balancer.hpp"

namespace tair {
namespace storage {
namespace ldb {
//////////////////// LdbBalancer::Balancer
LdbBalancer::Balancer::Balancer(LdbBalancer *owner, LdbManager *manager) :
        owner_(owner), manager_(manager) {
}

LdbBalancer::Balancer::~Balancer() {
    owner_->finish(this);
}

void LdbBalancer::Balancer::run(tbsys::CThread *thread, void *arg) {
    pthread_detach(pthread_self());

    BucketIndexer::INDEX_BUCKET_MAP index_map;
    // get current bucket index map
    manager_->get_index_map(index_map);
    // get unit to be balanced
    std::vector<Unit> units;
    try_balance(index_map, units);
    if (!units.empty()) {
        do_balance(units);
    }
    delete this;
}

void
LdbBalancer::Balancer::try_balance(const BucketIndexer::INDEX_BUCKET_MAP &index_map, std::vector<Unit> &result_units) {
    result_units.clear();

    int32_t bucket_count = 0;
    for (BucketIndexer::INDEX_BUCKET_MAP::const_iterator it = index_map.begin();
         it != index_map.end();
         ++it) {
        bucket_count += it->size();
    }
    if (bucket_count == 0) {
        return;
    }

    int32_t average = bucket_count / index_map.size();
    int32_t remainder = bucket_count % index_map.size();

    BucketIndexer::INDEX_BUCKET_MAP result_index_map(index_map.size());

    // first pull out
    for (size_t index = 0; index < index_map.size(); ++index) {
        std::vector<int32_t> buckets = index_map[index];
        // avoid sequential buckets
        std::random_shuffle(buckets.begin(), buckets.end());

        std::vector<int32_t>::iterator bit = buckets.begin();
        int32_t more = buckets.size() - average;

        if (more > 0) {
            if (remainder > 0) {
                remainder--;
                more--;
            }
            if (more > 0) {
                // this bucket need balance later
                for (int i = 0; i < more; ++i) {
                    // pull out the more
                    result_units.push_back(Unit(*bit++, index, 0));
                }
            }
        }

        // add out-of-balance ones to result index map
        result_index_map[index].insert(result_index_map[index].end(), bit, buckets.end());
    }

    if (result_units.empty()) {
        log_warn("NO NEED balance, input: %s", BucketIndexer::to_string(index_map).c_str());
    } else {
        // second push in
        // avoid too A => B
        std::random_shuffle(result_units.begin(), result_units.end());

        std::vector<Unit>::iterator uit = result_units.begin();
        for (size_t index = 0; index < result_index_map.size(); ++index) {
            std::vector<int32_t> &buckets = result_index_map[index];
            int32_t less = average - buckets.size();
            if (less >= 0) {
                if (remainder > 0) {
                    less++;
                    remainder--;
                }

                if (less > 0) {
                    // balance some
                    for (int32_t i = 0; i < less; ++i, ++uit) {
                        uit->to_ = index;
                        buckets.push_back(uit->bucket_);
                    }
                }
            }
        }

        std::string units_str;
        for (std::vector<Unit>::iterator it = result_units.begin(); it != result_units.end(); ++it) {
            units_str.append(it->to_string());
        }

        log_warn("NEED balance. input: %s, output: %s, balance units: (%lu) %s",
                 BucketIndexer::to_string(index_map).c_str(),
                 BucketIndexer::to_string(result_index_map).c_str(),
                 result_units.size(), units_str.c_str());
    }
}

int LdbBalancer::Balancer::do_balance(const std::vector<Unit> &units) {
    int ret = TAIR_RETURN_SUCCESS;
    for (size_t i = 0; i < units.size() && !_stop; ++i) {
        log_warn("start one balance: (%lu/%lu) %s", i + 1, units.size(), units[i].to_string().c_str());
        ret = do_one_balance(units[i]);
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("balance one bucket fail: %s, ret: %d", units[i].to_string().c_str(), ret);
            break;
        }
    }

    return ret;
}

int LdbBalancer::Balancer::do_one_balance(const Unit &unit) {
    int32_t bucket = unit.bucket_;
    leveldb::DB *from_db = manager_->get_instance(unit.from_)->db();
    leveldb::DB *to_db = manager_->get_instance(unit.to_)->db();

    int32_t batch_size = 0;
    int64_t item_count = 0, data_size = 0;
    leveldb::WriteBatch batch;

    leveldb::WriteOptions write_options;
    write_options.sync = false;
    leveldb::Status status;

    int32_t start_time = time(NULL);

    // all writes is synced because all data's
    // job(rsync eg.) has been done in from_db
    const bool synced = true;

    // process
    typedef enum {
        // balancing is doing(data is moving)
                DOING = 0,
        // data moving is almost over, propose to switch
                PROPOSING,
        // all done, commit over
                COMMIT,
    } Process;

    // input data iterator
    LdbBucketDataIter data_it(bucket, from_db);
    data_it.seek_to_first();

    // here we go...
    Process process = DOING;
    while (process != COMMIT && !_stop) {
        if (!data_it.valid()) {
            if (process == DOING) {
                // we got the last record, just pause write for switch.
                // Actually, data_it chases the normal write request,
                // we just expect data_it is faster.
                // TODO: stop write earlier if we are tired
                //       of this chasing game(already be closer enough eg.).
                manager_->pause_service(bucket);
                // try to get the possible remaining data
                data_it.next();
                // now, we are proposing to be done
                process = PROPOSING;
            } else if (process == PROPOSING) {
                // Now, all data has been got really,
                // write last batch if existed
                if (batch_size > 0) {
                    // trigger last write
                    batch_size = MAX_BATCH_SIZE + 1;
                }
                // all done
                process = COMMIT;
            }
        }

        if (data_it.valid()) {
            leveldb::Slice &key = data_it.key();
            leveldb::Slice &value = data_it.value();
            char type = leveldb::OffSyncMask(data_it.type());

            batch_size += key.size() + value.size();
            data_size += key.size() + value.size();
            ++item_count;

            switch (type) {
                case leveldb::kTypeValue:
                    batch.Put(key, value, synced);
                    break;
                case leveldb::kTypeDeletion:
                    batch.Delete(key, synced);
                    break;
                case leveldb::kTypeDeletionWithTailer:
                    batch.Delete(key, value, synced);
                    break;
                default:
                    log_error("unknown record type: %d", type);
                    break;
            }

            data_it.next();
        }

        if (batch_size > MAX_BATCH_SIZE) {
            while (!_stop) {
                status = to_db->Write(write_options, &batch, bucket);
                if (status.ok()) {
                    break;
                }

                if (status.IsSlowWrite()) {
                    TAIR_SLEEP(_stop, 2);
                } else {
                    log_error("write batch fail: %s", status.ToString().c_str());
                    break;
                }
            }
            if (!status.ok()) {
                break;
            }

            batch_size = 0;
            batch.Clear();

            int64_t wait_us = owner_->get_wait_us();
            if (process <= DOING && wait_us > 0) {
                // too much love will kill you. silly wait
                if (wait_us > 1000000) {
                    TAIR_SLEEP(_stop, wait_us / 1000000);
                } else {
                    ::usleep(wait_us);
                }
            }
        }
    }

    int ret = status.ok() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;

    if (process >= COMMIT && ret == TAIR_RETURN_SUCCESS) {
        // data written by spcified bucket can't be seen
        // when data is still in memtable(ForceCompactMemTable() is asynced).
        // TODO: consummate update-with-bucket to fix this.
        to_db->ForceCompactMemTable();
        ret = manager_->reindex_bucket(bucket, unit.from_, unit.to_);
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("reindex bucket %d fail, ret: %d", bucket, ret);
        }
    }

    if (process >= PROPOSING) {
        // has paused, must resume
        manager_->resume_service(bucket);
    }

    log_warn("balance %s, itemcount: %"PRI64_PREFIX"d, datasize: %"PRI64_PREFIX"d, process: %d, cost: %ld(s), suc: %s",
             unit.to_string().c_str(), item_count, data_size, process, time(NULL) - start_time,
             (process == COMMIT && ret == TAIR_RETURN_SUCCESS) ? "yes" : "no");

    return ret;
}

//////////////////// LdbBalancer
LdbBalancer::LdbBalancer(LdbManager *manager) :
        manager_(manager), balancer_(NULL), wait_us_(1000) {}

LdbBalancer::~LdbBalancer() {
    if (balancer_ != NULL) {
        balancer_->stop();
        // we need wait for balancer finish
        // we can't use wait() `cause balancer_
        // has detached itself
        while (balancer_ != NULL) {
            ::usleep(10);
        }
    }
}

void LdbBalancer::start() {
    if (balancer_ != NULL) {
        log_warn("balance already in process");
    } else {
        balancer_ = new Balancer(this, manager_);
        balancer_->start();
        log_warn("balance start.");
    }
}

void LdbBalancer::stop() {
    if (balancer_ == NULL) {
        log_warn("no balance in process");
    } else {
        balancer_->stop();
    }
}

void LdbBalancer::set_wait_us(int64_t us) {
    log_warn("set balance wait us: %"PRI64_PREFIX"d", us);
    wait_us_ = us;
}

int64_t LdbBalancer::get_wait_us() {
    return wait_us_;
}

void LdbBalancer::finish(Balancer *balancer) {
    if (balancer == balancer_) {
        balancer_ = NULL;
        BucketIndexer::INDEX_BUCKET_MAP index_map;
        manager_->get_index_map(index_map);
        log_warn("balance finish. current index map: %s", BucketIndexer::to_string(index_map).c_str());
    }
}

}
}
}
