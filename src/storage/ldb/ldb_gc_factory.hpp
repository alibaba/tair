/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb gc bucket or area.
 * Synchronization guaranteed, cause add/check may happen in different place,
 * outside synchronization can't be got.
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_GC_FACTORY_H
#define TAIR_STORAGE_LDB_GC_FACTORY_H

#include <stdint.h>
#include <ext/hash_map>

#include <tbsys.h>
#include "common/file_op.hpp"
#include "ldb_define.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      enum GcOper
      {
        GC_ADD = 1,
        GC_RM = 2
      };

      enum GcType
      {
        GC_BUCKET = 1,
        GC_AREA = 2
      };

      struct GcNode
      {
        GcNode() {}
        GcNode(int32_t key, uint64_t sequence = 0, uint64_t file_number = 0, uint32_t when = 0)
          : key_(key), sequence_(sequence), file_number_(file_number), when_(when) {}

        void encode(char* buf) const
        {
          tair::util::coding_util::encode_fixed32(buf, key_);
          buf += sizeof(int32_t);
          tair::util::coding_util::encode_fixed64(buf, sequence_);
          buf += sizeof(uint64_t);
          tair::util::coding_util::encode_fixed64(buf, file_number_);
          buf += sizeof(uint64_t);
          tair::util::coding_util::encode_fixed32(buf, when_);
        }

        void decode(const char* buf)
        {
          key_ = tair::util::coding_util::decode_fixed32(buf);
          buf += sizeof(int32_t);
          sequence_ = tair::util::coding_util::decode_fixed64(buf);
          buf += sizeof(uint64_t);
          file_number_ = tair::util::coding_util::decode_fixed64(buf);
          buf += sizeof(uint64_t);
          when_ = tair::util::coding_util::decode_fixed32(buf);
        }

        bool operator ==(const GcNode& right) const
        {
          return (right.key_ == key_ && right.sequence_ == sequence_ &&
                  right.file_number_ == file_number_ && right.when_ == when_);
        }

        int32_t key_;
        uint64_t sequence_;
        uint64_t file_number_;
        uint32_t when_;
      };

      struct GcLogRecord
      {
        GcLogRecord() {}
        GcLogRecord(GcOper oper_type, GcType node_type, const GcNode& node)
          : oper_type_(oper_type), node_type_(node_type), node_(node) {}

        void encode(char* buf)
        {
          buf[0] = static_cast<char>(oper_type_);
          buf[1] = static_cast<char>(node_type_);
          node_.encode(buf + 2);
        }

        void decode(const char* buf)
        {
          oper_type_ = static_cast<GcOper>(buf[0]);
          node_type_ = static_cast<GcType>(buf[1]);
          node_.decode(buf + 2);
        }

        GcOper oper_type_;
        GcType node_type_;
        GcNode node_;
      };

      static const int32_t GC_LOG_IO_TRY = 5;
      static const int32_t GC_LOG_HEADER_SIZE = 64; // reserved
      static const int32_t GC_LOG_RECORD_SIZE = sizeof(GcLogRecord);

#undef GCNODE_SELF_FMT
#undef GCNODE_SELF_ARGS
#undef DUMP_GCNODE

#define GCNODE_SELF_FMT                                                 \
      ", node [key: %d, sequence: %"PRI64_PREFIX"u, filenumber: %"PRI64_PREFIX"u, when: %u]."

#define GCNODE_SELF_ARGS(NODE)                                          \
      (NODE).key_, (NODE).sequence_, (NODE).file_number_, (NODE).when_

#define DUMP_GCNODE(level, node, fmt, args...)                          \
      (log_##level(fmt GCNODE_SELF_FMT, ##args, GCNODE_SELF_ARGS(node)))

      // area/bucket ==> GcNode (maybe set)
      typedef __gnu_cxx::hash_map<int32_t, GcNode> GC_MAP;
      typedef GC_MAP::iterator GC_MAP_ITER;
      typedef GC_MAP::const_iterator GC_MAP_CONST_ITER;

      class LdbInstance;
      class LdbGcFactory;

      // no synchronization gurantee
      class GcLog
      {
      public:
        GcLog();
        ~GcLog();

        bool start(const char* file_name, LdbGcFactory* target, bool try_replay = true);
        void stop();
        void destroy();
        int flush();

        int add(GcOper oper_type, GcType node_type, const GcNode& node);
        int add(const char* buf, int32_t size);

        int rename(const char* new_name);

      private:
        int replay_apply(LdbGcFactory* target);
        bool check_init();
        int32_t log_record_count();
        int write_header();

      private:
        tair::common::FileOperation* file_;
        int64_t file_offset_;
      };

      class LdbGcFactory
      {
      public:
        LdbGcFactory(LdbInstance* db);
        ~LdbGcFactory();
        friend class LdbComparatorImpl; // convient use

        bool start();
        void stop();
        void destroy();

        bool empty();
        bool empty(GcType type);
        int add(int32_t key, GcType type);
        int add(const std::vector<int32_t>& keys, GcType type);
        // before remove, the key => node maybe modifid,
        // we can only remove node that is exact as @node from factory.
        int remove(const GcNode& node, GcType type);

        GcNode pick_gc_node(GcType type);
        void try_evict();

        // check whether this key need gc, user should hold lock_(LdbComparator convient use)
        bool need_gc(int32_t key, uint64_t sequence, const GC_MAP& container);
        // apply record when replay log, user should hold lock_
        int apply(const GcLogRecord& record);

        void debug_string();
        void clear() { gc_buckets_.clear(); gc_areas_.clear();}
        void pause_gc();
        void resume_gc();
        bool can_gc() { return can_gc_; }

      private:
        void set_gc_info(GcNode& node);
        int add(int32_t key, GcType type, GC_MAP& gc_container);
        int add(const std::vector<int32_t>& keys, GcType type, GC_MAP& gc_container);
        int remove(const GcNode& node, GcType type, GC_MAP& gc_container);

        GcNode pick_gc_node(const GC_MAP& gc_container);
        void try_evict(GC_MAP& gc_container, GcType type, uint64_t limit_file_number);

        int rotate_log();
        std::string gc_log_name();
        std::string gc_log_rotate_name();

        void dump(char* buf, GcType type, const GC_MAP& gc_container);
        int apply_remove_gc_node(GcType type, const GcNode& node);
        int apply_add_gc_node(GcType type, const GcNode& node);

      private:
        tbsys::CRWLock lock_;
        LdbInstance* db_;
        GC_MAP gc_buckets_;
        GC_MAP gc_areas_;
        GcLog* log_;
        bool can_gc_;
      };
    }
  }
}

#endif
