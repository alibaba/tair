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

#ifndef TAIR_STORAGE_LDB_COMPARATOR_H
#define TAIR_STORAGE_LDB_COMPARATOR_H

#include "leveldb/comparator.h"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      class LdbGcFactory;
      class BitcmpLdbComparatorImpl : public leveldb::Comparator
      {
      public:
        BitcmpLdbComparatorImpl();
        explicit BitcmpLdbComparatorImpl(LdbGcFactory* gc);
        virtual ~BitcmpLdbComparatorImpl();
        virtual int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;
        virtual const char* Name() const;
        virtual void FindShortestSeparator(
          std::string* start,
          const leveldb::Slice& limit) const;
        virtual void FindShortSuccessor(std::string* key) const;
        // should drop this key no matter what condition.(user defined)
        virtual bool ShouldDrop(const char* key, int64_t sequence, uint32_t will_gc = 0) const;
        // should drop this key based on some condition. (user defined)
        virtual bool ShouldDropMaybe(const char* key, int64_t sequence, uint32_t now = 0) const;
      private:
        LdbGcFactory* gc_;
      };

      class NumericalComparatorImpl : public leveldb::Comparator
      {
      public:
        NumericalComparatorImpl();
        explicit NumericalComparatorImpl(LdbGcFactory* gc, const char start, size_t len);
        virtual ~NumericalComparatorImpl();
        virtual int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;
        virtual const char* Name() const;
        virtual void FindShortestSeparator(
          std::string* start,
          const leveldb::Slice& limit) const;
        virtual void FindShortSuccessor(std::string* key) const;
        // should drop this key no matter what condition.(user defined)
        virtual bool ShouldDrop(const char* key, int64_t sequence, uint32_t will_gc = 0) const;
        // should drop this key based on some condition. (user defined)
        virtual bool ShouldDropMaybe(const char* key, int64_t sequence, uint32_t now = 0) const;
      private:
        //const char *PrefixCheck(const char *key, size_t len) const;
        //const char *KeyToNumber(const char *key, size_t len, int64_t &number, const char *&number_p) const;
        int StringCompare(const char *a, size_t len_a, const char *b, size_t len_b) const;
        int64_t Strntoul(const char *key, size_t len, const char **endptr) const;
        const char *FindNumber(const char *key, size_t len) const;
        const char *MetaSkip(const char *key, size_t len) const;
      private:
        const char number_delim_; //special char befor number
        size_t meta_len_;       //skip meta_len_
        leveldb::Comparator* ldb_comparator_; 
      }; 

      extern const leveldb::Comparator* LdbComparator(LdbGcFactory* gc);
    }
  }
}
#endif






