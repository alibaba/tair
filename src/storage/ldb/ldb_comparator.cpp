/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */
#include "common/define.hpp"
#include "ldb_define.hpp"
#include "ldb_gc_factory.hpp"
#include "ldb_comparator.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      static const size_t LDB_COMPARE_SKIP_SIZE = LDB_EXPIRED_TIME_SIZE;
      BitcmpLdbComparatorImpl::BitcmpLdbComparatorImpl() : gc_(NULL)
      {
      }

      BitcmpLdbComparatorImpl::BitcmpLdbComparatorImpl(LdbGcFactory* gc) : gc_(gc)
      {
      }

      BitcmpLdbComparatorImpl::~BitcmpLdbComparatorImpl()
      {
      }

      const char* BitcmpLdbComparatorImpl::Name() const
      {
        return "ldb.bitcmpLdbComparator";
      }

      // skip expired time
      int BitcmpLdbComparatorImpl::Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
      {
        assert(a.size() > LDB_COMPARE_SKIP_SIZE && b.size() > LDB_COMPARE_SKIP_SIZE);
        const int min_len = (a.size() < b.size()) ? a.size() - LDB_COMPARE_SKIP_SIZE :
          b.size() - LDB_COMPARE_SKIP_SIZE;
        int r = memcmp(a.data() + LDB_COMPARE_SKIP_SIZE, b.data() + LDB_COMPARE_SKIP_SIZE, min_len);
        if (r == 0)
        {
          if (a.size() < b.size())
          {
            r = -1;
          }
          else if (a.size() > b.size())
          {
            r = +1;
          }
        }
        return r;
      }

      // skip expired time
      void BitcmpLdbComparatorImpl::FindShortestSeparator(
        std::string* start,
        const leveldb::Slice& limit) const
      {
        // Find length of common prefix
        size_t min_length = std::min(start->size(), limit.size());
        assert(min_length > LDB_COMPARE_SKIP_SIZE);
        size_t diff_index = LDB_COMPARE_SKIP_SIZE;
        while ((diff_index < min_length) &&
               ((*start)[diff_index] == limit[diff_index]))
        {
          diff_index++;
        }

        if (diff_index >= min_length) {
          // Do not shorten if one string is a prefix of the other
        }
        else
        {
          uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
          if (diff_byte < static_cast<uint8_t>(0xff) &&
              diff_byte + 1 < static_cast<uint8_t>(limit[diff_index]))
          {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            assert(Compare(*start, limit) < 0);
          }
        }
      }

      void BitcmpLdbComparatorImpl::FindShortSuccessor(std::string* key) const
      {
        // Find first character that can be incremented
        size_t n = key->size();
        for (size_t i = LDB_COMPARE_SKIP_SIZE; i < n; i++)
        {
          const uint8_t byte = (*key)[i];
          if (byte != static_cast<uint8_t>(0xff))
          {
            (*key)[i] = byte + 1;
            key->resize(i+1);
            return;
          }
        }
        // *key is a run of 0xffs.  Leave it alone.
      }

      bool BitcmpLdbComparatorImpl::ShouldDrop(const char* key, int64_t sequence, uint32_t will_gc) const
      {
        if (gc_ == NULL)
        {
          return false;
        }
        // Epired items can't drop only if it is the only key update here,
        // it should be check in ShouldDropMaybe(). eg:
        //  1) insert a => b
        //  2) insert a => c expired time 10s
        //  We can drop this key (a => c) only if (a => b) has droped(compacted by same entry).
        //  Gc factory's check depends on no condition here, because every items will be checked
        //  by gc.

        // key format
        //     expired_time  fixed32
        //     bucket_number 3bytes
        //     area_number   2bytes
        //     user_key      ...

        bool drop = false;
        // this test will NOT gc this key or
        // will gc and gc_factory allow gc,
        // then we will do the test
        if (0 == will_gc || (0 != will_gc && gc_->can_gc_))
        {
          const char* pkey = key;
          pkey += LDB_EXPIRED_TIME_SIZE;

          // To decode as later as possible, check empty and check need_gc separately, so need lock here.
          tbsys::CRLockGuard guard(gc_->lock_);
          if (!gc_->gc_buckets_.empty()) // 1. check if bucket is valid. can not use empty(GC_BUCKET)
          {
            drop = gc_->need_gc(LdbKey::decode_bucket_number(pkey), sequence, gc_->gc_buckets_);
            // fprintf(stderr, "drop bucket: %d %lu %s\n", LdbKey::decode_bucket_number(pkey), sequence, pkey + 5);
          }
          if (!drop && !gc_->gc_areas_.empty()) // 2. check if area is valid
          {
            // area decode, consensus with data_entry
            drop = gc_->need_gc(LdbKey::decode_area(pkey + LDB_KEY_BUCKET_NUM_SIZE), sequence, gc_->gc_areas_);
            // fprintf(stderr, "drop %d area: %d\n", drop, LdbKey::decode_area(pkey + LDB_KEY_BUCKET_NUM_SIZE));
          }
        }
        return drop;
      }

      bool BitcmpLdbComparatorImpl::ShouldDropMaybe(const char* key, int64_t sequence, uint32_t now) const
      {
        UNUSED(sequence);
        // check expired time here. see ShouldDrop()
        uint32_t expired_time = tair::util::coding_util::decode_fixed32(key);
        return expired_time > 0 && expired_time < (now > 0 ? now : time(NULL));
      }


      const leveldb::Comparator* LdbComparator(LdbGcFactory* gc)
      {
        const char *comparator_type = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_COMPARATOR_TYPE, "");
        if (strcmp(comparator_type, "numeric") == 0)
        {
          int meta_len = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USERKEY_SKIP_META_SIZE, 0);
          const char *delimiter = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_USERKEY_NUM_DELIMITER, " ");
          log_warn("using numerical Comparator. num_start:%c, meta_len:%d",delimiter[0], meta_len);
          return new NumericalComparatorImpl(gc, delimiter[0], meta_len);
        }
        else if (strcmp(comparator_type, "bitcmp") == 0)
        {
          return new BitcmpLdbComparatorImpl(gc);
        }
        else
        {
          log_warn("no such comparator type: %s. use bitcmpLdbComparator by defalut",comparator_type);
          return new BitcmpLdbComparatorImpl(gc);
        }
      }
    }
  }
}
