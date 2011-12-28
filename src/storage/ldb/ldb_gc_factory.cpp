/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb gc bucket or area
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/define.hpp"
#include "common/log.hpp"
#include "ldb_instance.hpp"
#include "ldb_gc_factory.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using namespace tair::common;
//////////////////////////////
      GcLog::GcLog() : file_(NULL)
      {
      }

      GcLog::~GcLog()
      {
        if (file_ != NULL)
        {
          delete file_;
        }
      }

      bool GcLog::start(const char* name, LdbGcFactory* target, bool try_replay)
      {
        bool ret = name != NULL && target != NULL;
        if (ret)
        {
          if (file_ != NULL)
          {
            log_warn("gc log already init, filename: %s", file_->get_file_name());
          }
          else
          {
            bool need_new_log = !try_replay || ::access(name, F_OK) != 0;
            file_ = new tair::common::FileOperation(name);
            // not lazy open to get error when init
            if (file_->open_file() < 0)
            {
              log_error("open log file %s fail, error: %s", name, strerror(errno));
              ret = false;
            }
            else
            {
              int tmp_ret = TAIR_RETURN_SUCCESS;
              // replay
              if (!need_new_log && (tmp_ret = replay_apply(target) != TAIR_RETURN_SUCCESS))
              {
                log_error("gc log file exist but replay fail, ignore");
                need_new_log = true;
              }

              if (need_new_log)
              {
                tmp_ret = write_header();
                if (tmp_ret != TAIR_RETURN_SUCCESS)
                {
                  log_error("write gc header fail, ret: %d", tmp_ret);
                  ret = false;
                }
              }
            }

            if (!ret)            // rollback
            {
              log_error("start gc log fail: %s", name);
              if (file_ != NULL)
              {
                file_->unlink_file();
                delete file_;
                file_ = NULL;
              }
              file_offset_  = 0;
            }
            else
            {
              log_info("start gc log succuess: %s, fileoffset: %"PRI64_PREFIX"d", name, file_offset_);
            }
          }
        }

        return ret;
      }

      void GcLog::stop()
      {
        flush();
      }

      void GcLog::destroy()
      {
        if (check_init())
        {
          if (file_->unlink_file() != 0)
          {
            log_error("unlink gc log file fail. file: %s, error: %s", file_->get_file_name(), strerror(errno));
          }
          log_info("destroy gc log");
        }
      }

      int GcLog::flush()
      {
        int ret = check_init() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        if (TAIR_RETURN_SUCCESS == ret)
        {
          if (file_->flush_file() != 0)
          {
            log_error("sync gc log file fail, error: %s", strerror(errno));
            ret = TAIR_RETURN_FAILED;
          }
        }
        return ret;
      }

      int GcLog::add(GcOper oper_type, GcType node_type, const GcNode& node)
      {
        int ret = check_init() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("add gc record fail. gc log file not init");
        }
        else
        {
          char buf[GC_LOG_RECORD_SIZE];
          GcLogRecord(oper_type, node_type, node).encode(buf);

          int32_t write_len = 0;
          if ((write_len = file_->pwrite_file(buf, GC_LOG_RECORD_SIZE, file_offset_)) != GC_LOG_RECORD_SIZE)
          {
            ret = TAIR_RETURN_FAILED;
            log_error("add gclog fail, write fail. ret: %d", write_len);
          }
        }

        if (TAIR_RETURN_SUCCESS == ret)
        {
          DUMP_GCNODE(info, node, "add gc log record succuess, opertype: %d, nodetype: %d",
                      oper_type, node_type);
          file_offset_ += GC_LOG_RECORD_SIZE;
        }
        else
        {
          DUMP_GCNODE(error, node, "add gc log record fail, opertype: %d, nodetype: %d",
                      oper_type, node_type);
        }

        return ret;
      }

      int GcLog::add(const char* buf, int32_t size)
      {
        int ret = buf != NULL && size > 0 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        int32_t write_len = 0;
        if (TAIR_RETURN_SUCCESS != ret)
        {
          log_error("add gc log invaid buffer. size: %d", size);
        }
        else if ((write_len = file_->pwrite_file(buf, size, file_offset_)) != size)
        {
          ret = TAIR_RETURN_FAILED;
          log_error("add buf to gc log fail, size: %d, ret: %d", size, write_len);
        }
        else
        {
          file_offset_ += size;
          log_info("add buf to gc log succuess, size: %d", size);
        }
        return ret;
      }

      int GcLog::rename(const char* new_name)
      {
        int ret = check_init() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        if (TAIR_RETURN_SUCCESS == ret)
        {
          ret = file_->rename_file(new_name);
        }
        return ret;
      }

      int GcLog::replay_apply(LdbGcFactory* target)
      {
        int ret = check_init() && target != NULL ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        log_info("replay apply gc log: %s", file_->get_file_name());
        if (ret == TAIR_RETURN_SUCCESS)
        {
          int32_t record_count = log_record_count();
          if (record_count < 0)
          {
            log_error("replay gclog fail, file %s is invalid", file_->get_file_name());
            ret = TAIR_RETURN_FAILED;
          }
          else
          {
            target->clear();
            if (0 == record_count)
            {
              log_info("no gc log record replay");
            }
            else
            {
              // read once should ok.
              char* buf = new char[GC_LOG_RECORD_SIZE * record_count];
              int32_t read_len = 0;
              if ((read_len = file_->pread_file(buf, GC_LOG_RECORD_SIZE * record_count, GC_LOG_HEADER_SIZE)) !=
                  GC_LOG_RECORD_SIZE * record_count)
              {
                ret = TAIR_RETURN_FAILED;
                log_error("replay gclog fail. read log fail. ret: %d", read_len);
              }
              else
              {
                GcLogRecord record;
                const char* pbuf = buf;
                for (int32_t i = 0; i < record_count; ++i)
                {
                  record.decode(pbuf);
                  target->apply(record);
                  pbuf += GC_LOG_RECORD_SIZE;
                }
              }
              target->debug_string();
              delete[] buf;
            }
            // set file end
            file_offset_ = GC_LOG_HEADER_SIZE + GC_LOG_RECORD_SIZE * record_count;
          }
        }

        return ret;
      }

      bool GcLog::check_init()
      {
        return file_ != NULL;
      }

      int32_t GcLog::log_record_count()
      {
        // use file size to get record count.
        // log header may contain this later.
        int32_t count = check_init() ? 0 : -1;
        int64_t file_size = 0;
        if (count >= 0)
        {
          file_size = file_->get_file_size();
          if (file_size < 0)
          {
            log_warn("get gc log file fail, ignore");
            count = -1;
          }
          else if (file_size < GC_LOG_HEADER_SIZE)
          {
            log_error("gc log file is invalid, less than header size: %"PRI64_PREFIX"d", file_size);
            count = 0;
          }
          else
          {
            if ((file_size - GC_LOG_HEADER_SIZE) % GC_LOG_RECORD_SIZE != 0)
            {
              log_error("gc log file maybe conflict. filesize: %"PRI64_PREFIX"d, replay most", file_size);
            }
            count = (file_size - GC_LOG_HEADER_SIZE) / GC_LOG_RECORD_SIZE;
          }
        }
        log_info("gc log file size: %"PRI64_PREFIX"d, record count: %d", file_size, count);
        return count;
      }

      int GcLog::write_header()
      {
        int ret = check_init() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        if (TAIR_RETURN_SUCCESS == ret)
        {
          // TODO: some magic maybe. reserve now
          if (file_->ftruncate_file(GC_LOG_HEADER_SIZE) != 0)
          {
            log_error("write header fail. error: %s", strerror(errno));
            file_offset_ = 0;
          }
          else
          {
            file_offset_ = GC_LOG_HEADER_SIZE;
          }
        }
        return ret;
      }

//////////////////////////////
      LdbGcFactory::LdbGcFactory(LdbInstance* db) : db_(db)
      {
        assert(db_ != NULL);
        log_ = new GcLog();
      }

      LdbGcFactory::~LdbGcFactory()
      {
        stop();
        if (log_ != NULL)
        {
          delete log_;
        }
      }

      bool LdbGcFactory::start()
      {
        tbsys::CThreadGuard guard(&lock_);
        bool ret = db_ != NULL && db_->db_path();
        if (!ret)
        {
          log_error("start gc fail. db not init or db path not init");
        }
        else if (!(ret = log_->start(gc_log_name().c_str(), this)))
        {
          log_error("start gc log fail. db path: %s", gc_log_name().c_str());
        }
        else if (!empty())      // have gc, then rotate gc log
        {
          if (rotate_log() != TAIR_RETURN_SUCCESS)
          {
            log_error("start::rotate gc log fail. just ignore"); // we have valid gc info now
          }
        }
        return ret;
      }

      void LdbGcFactory::stop()
      {
        tbsys::CThreadGuard guard(&lock_);
        if (db_ != NULL)
        {
          if (empty())
          {
            log_->destroy();
          }
          else
          {
            rotate_log();
          }

          log_->stop();
          db_ = NULL;
        }
      }

      void LdbGcFactory::destroy()
      {
        if (log_ != NULL)
        {
          log_->destroy();
        }
      }

      bool LdbGcFactory::empty()
      {
        // not lock
        return gc_buckets_.empty() && gc_areas_.empty();
      }

      bool LdbGcFactory::empty(GcType type)
      {
        // not lock
        bool ret = false;

        switch (type) {
        case GC_BUCKET:
          ret = gc_buckets_.empty();
          break;
        case GC_AREA:
          ret = gc_areas_.empty();
          break;
        default:
          log_error("check empty, invalid gc type: %d", type);
          break;
        }

        return ret;
      }

      int LdbGcFactory::add(int32_t key, GcType type)
      {
        tbsys::CThreadGuard guard(&lock_);
        int ret = TAIR_RETURN_SUCCESS;
        log_info("add gc key: %d, type: %d", key, type);

        switch (type) {
        case GC_BUCKET:
          ret = add(key, type, gc_buckets_);
          break;
        case GC_AREA:
          ret = add(key, type, gc_areas_);
          break;
        default:
          log_error("add invalid gc type: %d", type);
          ret = TAIR_RETURN_FAILED;
          break;
        }

        return ret;
      }

      int LdbGcFactory::add(const std::vector<int32_t>& keys, GcType type)
      {
        tbsys::CThreadGuard guard(&lock_);
        int ret = TAIR_RETURN_SUCCESS;

        log_info("add gc keys size: %zd, type: %d", keys.size(), type);

        switch (type) {
        case GC_BUCKET:
          ret = add(keys, type, gc_buckets_);
          break;
        case GC_AREA:
          ret = add(keys, type, gc_areas_);
          break;
        default:
          log_error("add invalid gc type: %d", type);
          ret = TAIR_RETURN_FAILED;
          break;
        }
        return ret;
      }

      int LdbGcFactory::remove(const GcNode& node, GcType type)
      {
        tbsys::CThreadGuard guard(&lock_);
        int ret = TAIR_RETURN_SUCCESS;
        if (!empty(type))
        {
          switch (type) {
          case GC_BUCKET:
            ret = remove(node, type, gc_buckets_);
            break;
          case GC_AREA:
            ret = remove(node, type, gc_areas_);
            break;
          default:
            ret = TAIR_RETURN_FAILED;
            log_error("remove invalid gc type: %d", type);
            break;
          }
          if (empty())
          {
            rotate_log();
          }
        }
        return ret;
      }

      void LdbGcFactory::try_evict()
      {
        tbsys::CThreadGuard guard(&lock_);
        log_info("before try gc evict. buckets: %d, areas: %d", gc_buckets_.size(), gc_areas_.size());
        if (!empty())
        {
          uint64_t current_db_smallest_file_number = 0;
          get_db_stat(db_->db(), current_db_smallest_file_number, "smallest-filenumber");
          log_info("try evict gc. buckets: %d, area: %d, db smallest filenumber: %"PRI64_PREFIX"u",
                   gc_buckets_.size(), gc_areas_.size(), current_db_smallest_file_number);
          try_evict(gc_buckets_, GC_BUCKET, current_db_smallest_file_number);
          try_evict(gc_areas_, GC_AREA, current_db_smallest_file_number);
          if (empty())          // evict all, rotate new log.
          {
            rotate_log();
          }
        }
        log_info("after try gc evict. buckets: %d, areas: %d", gc_buckets_.size(), gc_areas_.size());

      }

      GcNode LdbGcFactory::pick_gc_node(GcType type)
      {
        tbsys::CThreadGuard guard(&lock_);
        GcNode node(-1);
        switch (type) {
        case GC_BUCKET:
          node = pick_gc_node(gc_buckets_);
          break;
        case GC_AREA:
          node = pick_gc_node(gc_areas_);
          break;
        default:
          log_error("pick invalid gc type: %d", type);
          break;
        }
        return node;
      }

      // user should hold lock_
      bool LdbGcFactory::need_gc(int32_t key, uint64_t sequence, const GC_MAP& container)
      {
        GC_MAP_CONST_ITER it = container.find(key);
        return (it != container.end() && it->second.sequence_ >= sequence);
      }

      // user should hold lock_
      int LdbGcFactory::apply(const GcLogRecord& record)
      {
        DUMP_GCNODE(info, record.node_, "apply gcrecord. oper_type: %d, node_type: %d",
                    record.oper_type_, record.node_type_);
        int ret = TAIR_RETURN_FAILED;
        switch (record.oper_type_) {
        case GC_ADD:
          ret = apply_add_gc_node(record.node_type_, record.node_);
          break;
        case GC_RM:
          ret = apply_remove_gc_node(record.node_type_, record.node_);
          break;
        default:
          log_error("apply invalid log record operate type: %d, ingore.", record.oper_type_);
          break;
        }
        return ret;
      }

      void LdbGcFactory::debug_string()
      {
        log_info("== debug gc factory info, gc buckets size: %d, gc areas size: %d ==",
                 gc_buckets_.size(), gc_areas_.size());
        for (GC_MAP_CONST_ITER it = gc_buckets_.begin(); it != gc_buckets_.end(); ++it)
        {
          DUMP_GCNODE(info, it->second, "type: GC_BUCKET");
        }
        for (GC_MAP_CONST_ITER it = gc_areas_.begin(); it != gc_areas_.end(); ++it)
        {
          DUMP_GCNODE(info, it->second, "type: GC_AREA");
        }
      }

      void LdbGcFactory::set_gc_info(GcNode& node)
      {
        get_db_stat(db_->db(), node.sequence_, "sequence");
        get_db_stat(db_->db(), node.file_number_, "largest-filenumber");
        node.when_ = time(NULL);
      }

      int LdbGcFactory::add(int32_t key, GcType type, GC_MAP& gc_container)
      {
        GcNode node(key, 0, 0, 0);
        set_gc_info(node);
        gc_container[key] = node;
        DUMP_GCNODE(info, node, "add gc node. type: %d, size: %zd", type, gc_container.size());
        return log_->add(GC_ADD, type, node);
      }

      int LdbGcFactory::add(const std::vector<int32_t>& keys, GcType type, GC_MAP& gc_container)
      {
        GcNode node;
        set_gc_info(node);

        for (std::vector<int32_t>::const_iterator it = keys.begin(); it != keys.end(); ++it)
        {
          node.key_ = *it;
          gc_container[*it] = node;
          DUMP_GCNODE(info, node, "add gc node. type: %d, size: %zd", type, gc_container.size());
          log_->add(GC_ADD, type, node); // error
        }
        return TAIR_RETURN_SUCCESS;
      }

      int LdbGcFactory::remove(const GcNode& node, GcType type, GC_MAP& gc_container)
      {
        int ret = TAIR_RETURN_SUCCESS;
        GC_MAP_ITER it = gc_container.find(node.key_);
        if (it != gc_container.end())
        {
          // when_ == 0 as remove anyway
          if (node.when_ == 0 || node == it->second)
          {
            ret = log_->add(GC_RM, type, it->second);
            gc_container.erase(it);
            DUMP_GCNODE(info, node, "remove gc node, type: %d", type);
          }
          else
          {
            // consider as succuess
            log_error("remove gcnode conflict, key: %d, type: %d", node.key_, type);
            DUMP_GCNODE(error, node, "remove node. type: %d", type);
            DUMP_GCNODE(error, it->second, "factory node. type: %d", type);
          }
        }
        return ret;
      }

      GcNode LdbGcFactory::pick_gc_node(const GC_MAP& gc_container)
      {
        return gc_container.empty() ? GcNode(-1) : gc_container.begin()->second;
      }

      void LdbGcFactory::try_evict(GC_MAP& gc_container, GcType type, uint64_t limit_file_number)
      {
        if (!gc_container.empty())
        {
          // maybe we can sort by filenumber
          for (GC_MAP_ITER it = gc_container.begin(); it != gc_container.end();)
          {
            if (it->second.file_number_ < limit_file_number)
            {
              DUMP_GCNODE(info, it->second, "evict gc node, type: %d, limit filenumber: %"PRI64_PREFIX"u",
                          type, limit_file_number);
              log_->add(GC_RM, type, it->second);
              gc_container.erase(it++);
            }
            else
            {
              ++it;
            }
          }
        }
      }

      void LdbGcFactory::dump(char* buf, GcType type, const GC_MAP& gc_container)
      {
        if (buf != NULL)
        {
          for (GC_MAP_CONST_ITER it = gc_container.begin(); it != gc_container.end(); ++it)
          {
            GcLogRecord(GC_ADD, type, it->second).encode(buf);
            buf += GC_LOG_RECORD_SIZE;
          }
        }
      }

      int LdbGcFactory::apply_add_gc_node(GcType type, const GcNode& node)
      {
        int ret = TAIR_RETURN_SUCCESS;
        switch (type) {
        case GC_BUCKET:
          gc_buckets_[node.key_] = node;
          break;
        case GC_AREA:
          gc_areas_[node.key_] = node;
          break;
        default:
          log_error("apply add invalid log record node type: %d, ignore.", type);
          ret = TAIR_RETURN_FAILED;
          break;
        }

        return ret;
      }

      int LdbGcFactory::apply_remove_gc_node(GcType type, const GcNode& node)
      {
        int ret = TAIR_RETURN_SUCCESS;
        switch (type) {
        case GC_BUCKET:
          gc_buckets_.erase(node.key_);
          break;
        case GC_AREA:
          gc_areas_.erase(node.key_);
          break;
        default:
          log_error("apply remove, invalid log record node type: %d, ignore.", type);
          ret = TAIR_RETURN_FAILED;
          break;
        }

        return ret;
      }

      int LdbGcFactory::rotate_log()
      {
        int ret = TAIR_RETURN_SUCCESS;

        if ((ret = log_->rename(gc_log_rotate_name().c_str())) != TAIR_RETURN_SUCCESS)
        {
          log_error("rotate fail. rename current log file fail. ret: %d", ret);
        }
        else
        {
          GcLog* new_log = new GcLog();

          if (!new_log->start(gc_log_name().c_str(), this, false))
          {
            log_error("rotate fail. start new log fail.");
            ret = TAIR_RETURN_FAILED;
          }
          else
          {
            if (!empty())
            {
              int32_t size = GC_LOG_RECORD_SIZE * (gc_buckets_.size() + gc_areas_.size());
              log_info("rotate gc log all record. buckets size: %d; areas size: %d, dumpbuffer size: %d",
                       gc_buckets_.size(), gc_areas_.size(), size);
              char* buf = new char[size];

              debug_string();

              dump(buf, GC_BUCKET, gc_buckets_);
              dump(buf + GC_LOG_RECORD_SIZE * gc_buckets_.size(), GC_AREA, gc_areas_);

              if ((ret = new_log->add(buf, size)) != TAIR_RETURN_SUCCESS)
              {
                log_error("rotate gc. add to log fail, ret: %d", ret);
              }
              delete[] buf;
            }
          }

          if (TAIR_RETURN_SUCCESS == ret)
          {
            log_->destroy();
            delete log_;
            log_ = new_log;
            log_info("rotate new gc log succuess");
          }
          else
          {
            log_error("rotate new log fail.");
            new_log->destroy();
            delete new_log;

            int tmp_ret;
            if ((tmp_ret = log_->rename(gc_log_name().c_str())) != TAIR_RETURN_SUCCESS)
            {
              log_error("rotate new log fail and current log can not rename. ret: %d, name: %s",
                        tmp_ret, gc_log_rotate_name().c_str());
            }
          }
        }

        return ret;
      }

      std::string LdbGcFactory::gc_log_name()
      {
        return db_ != NULL ? std::string(db_->db_path()) + "/ldb_gc_factory.log" : std::string();
      }

      std::string LdbGcFactory::gc_log_rotate_name()
      {
        return db_ != NULL ? std::string(db_->db_path()) + "/ldb_gc_factory.log.tmp" : std::string();
      }
    }
  }
}
