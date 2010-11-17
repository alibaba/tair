/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "fdb_item_manager.hpp"

namespace tair {
  namespace storage  {
    namespace fdb {
      fdb_item_manager::fdb_item_manager()
      {
        index_file = new file_operation();
        data_file = new file_operation();
        fb_manager = NULL;
        header = NULL;
      }

      fdb_item_manager::~fdb_item_manager()
      {
        if(fb_manager != NULL) {
          delete fb_manager;
            fb_manager = NULL;
        }

        delete data_file;
          data_file = NULL;

        delete index_file;
          index_file = NULL;
      }

      int fdb_item_manager::initialize(int bucket_number)
      {
        char file_name[FILENAME_MAX_LENGTH];
        const char *data_dir =
          TBSYS_CONFIG.getString(TAIRFDB_SECTION, FDB_DATA_DIR,
                                 FDB_DEFAULT_DATA_DIR);
        const char *db_name =
          TBSYS_CONFIG.getString(TAIRFDB_SECTION, FDB_NAME, FDB_DEFAULT_NAME);

        sprintf(file_name, "%s/%s_%06d.%s", data_dir, db_name, bucket_number,
                INDEX_SUFFIX);
        int flags = O_RDWR | O_CREAT | O_LARGEFILE;

        if(!index_file->open(file_name, flags, 0600)) {
          return INIT_FAILED;
        }

        sprintf(file_name, "%s/%s_%06d.%s", data_dir, db_name, bucket_number,
                DATA_SUFFIX);
        if(!data_file->open(file_name, flags, 0600)) {
          return INIT_FAILED;
        }

        bool is_new = false;
        if(index_file->is_empty()) {
          // init metadata
          is_new = true;
          db_header temp_header;
          dump_default_db_header(temp_header);
          char data[temp_header.idx_file_size];
          memset(data, 0, temp_header.idx_file_size);
          memcpy(data, &temp_header, DBHEADER_SIZE);
          if(!index_file->pwrite(data, temp_header.idx_file_size, 0)) {
            return INIT_FAILED;
          }

          char data_header[DBHEADER_MAGIC_SIZE];
          sprintf(data_header, "%s", DATA_MAGIC);
          if(!data_file->pwrite(data_header, DBHEADER_MAGIC_SIZE, 0)) {
            return INIT_FAILED;
          }
        }

        // load metadata
        int map_size =
          TBSYS_CONFIG.getInt(TAIRFDB_SECTION, FDB_INDEX_MMAP_SIZE, 128);
        map_size *= MB_SIZE;
        if(!index_file->mmap(map_size)) {
          return INIT_FAILED;
        }

        header = (struct db_header *) index_file->get_map_data();
        buckets =
          (uint32_t *) ((char *) index_file->get_map_data() + DBHEADER_SIZE);
        index_file->set_position(header->idx_file_size);

        int fb_count = is_new ? (1 << header->fb_power) : 0;
        char *pool =
          (char *) index_file->get_map_data() + DBHEADER_SIZE +
          FDB_BUCKET_NUMBER * sizeof(uint32_t);
        fb_manager = new free_blocks_manager(pool, fb_count);

        return is_new ? INIT_NEW : INIT_SUCC;

      }

      void fdb_item_manager::dump_default_db_header(db_header & header)
      {
        snprintf(header.magic, DBHEADER_MAGIC_SIZE, "%s", INDEX_MAGIC);
        header.flag = 0;
        header.fb_power =
          TBSYS_CONFIG.getInt(TAIRFDB_SECTION, FDB_FREE_BLOCK_POOL_SIZE, 8);
        header.item_count = 0;

        int fsize = DBHEADER_SIZE;        // header
        fsize += FDB_BUCKET_NUMBER * sizeof(uint32_t);        // bucket size
        fsize += 2 * sizeof(uint32_t);        // free block count | free block head
        fsize += (1 << header.fb_power) * FREEBLOCK_SIZE;        // free block pool size
        header.idx_file_size = fsize;
        header.dat_file_size = 16;
        header.free_idx_head = 0;

        memset(&header.stat, 0, sizeof(tair_pstat));
      }

      int fdb_item_manager::get_item(data_entry & key, fdb_item & ret_item)
      {

        uint32_t off = buckets[ret_item.bucket_index];

        fdb_item temp_item;
        memset(&temp_item, 0, sizeof(temp_item));
        uint32_t parent_off = 0;
        bool is_right = false;

        while(off > 0) {
          temp_item.meta_offset = off;
          if(!read_meta(temp_item)) {
            return TAIR_RETURN_FAILED;
          }

          if(ret_item.meta.hashcode > temp_item.meta.hashcode) {
            parent_off = off;
            is_right = true;
            off = temp_item.meta.right;
          }
          else if(ret_item.meta.hashcode < temp_item.meta.hashcode) {
            parent_off = off;
            is_right = false;
            off = temp_item.meta.left;
          }
          else {
            log_debug("hashcode is equal");
            char data_meta[FDB_ITEM_DATA_SIZE];
            if(!read_data(temp_item, data_meta, READ_DATA_META)) {
              return TAIR_RETURN_FAILED;
            }

            if(key.get_size() > temp_item.data.keysize) {
              parent_off = off;
              is_right = true;
              off = temp_item.meta.right;
            }
            else if(key.get_size() < temp_item.data.keysize) {
              parent_off = off;
              is_right = false;
              off = temp_item.meta.left;
            }
            else {
              log_debug("key size is equal");
              char data_key[temp_item.data.keysize];
              if(!read_data(temp_item, data_key, READ_DATA_KEY)) {
                return TAIR_RETURN_FAILED;
              }

              int cc = memcmp(key.get_data(), data_key, key.get_size());
              log_debug("memcmp result: %d", cc);
              if(cc > 0) {
                parent_off = off;
                is_right = true;
                off = temp_item.meta.right;
              }
              else if(cc < 0) {
                parent_off = off;
                is_right = false;
                off = temp_item.meta.left;
              }
              else {
                log_debug("key content is equal");
                // get then old item
                ret_item.meta_offset = temp_item.meta_offset;
                ret_item.meta = temp_item.meta;
                ret_item.data = temp_item.data;
                ret_item.parent_off = parent_off;
                ret_item.is_right = is_right;
                char *data_value = (char *) malloc(ret_item.data.valsize);
                assert(data_value != NULL);
                if(!read_data(temp_item, data_value, READ_DATA_VALUE)) {
                  log_debug("read data failed");
                  free(data_value);
                  return TAIR_RETURN_FAILED;
                }
                ret_item.value = data_value;
                return TAIR_RETURN_SUCCESS;
              }                        // end of key content equal
            }                        // end of key size equal
          }                        // end of hashcode equal

        }                        // end of while

        ret_item.parent_off = parent_off;
        ret_item.is_right = is_right;

        // data not exist
        return TAIR_RETURN_DATA_NOT_EXIST;
      }

      int fdb_item_manager::update_item(fdb_item & up_item)
      {
        if(!write_meta(up_item)) {
          return TAIR_RETURN_FAILED;
        }

        int rc =
          write_data(up_item) ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        return rc;
      }

      void fdb_item_manager::free_item(fdb_item & old_item)
      {
        free_meta(old_item);
        free_data(old_item);
      }

      void fdb_item_manager::free_meta(fdb_item & old_item)
      {
        uint64_t child_off = 0;

        old_item.log_self("item to free ");
        if(old_item.meta.left > 0 && old_item.meta.right < 1) {
          child_off = old_item.meta.left;
        }
        else if(old_item.meta.left < 1 && old_item.meta.right > 0) {
          child_off = old_item.meta.right;
        }
        else if(old_item.meta.left < 1 && old_item.meta.right < 1) {
          // no child at all
        }
        else {
          child_off = old_item.meta.left;
          uint64_t right_off = old_item.meta.right;

          fdb_item ritem;
          ritem.meta.right = old_item.meta.left;
          while(ritem.meta.right > 0) {
            ritem.meta_offset = ritem.meta.right;
            read_meta(ritem);
          }

          log_debug("item removed from tree");
          ritem.meta.right = right_off;
          write_meta(ritem);
        }

        if(old_item.parent_off > 0) {
          fdb_item pitem;
          pitem.meta_offset = old_item.parent_off;
          read_meta(pitem);
          if(old_item.is_right) {
            pitem.meta.right = child_off;
          }
          else {
            pitem.meta.left = child_off;
          }

          write_meta(pitem);
          log_debug("update parent at offset: %d", pitem.meta_offset);
        }
        else {
          buckets[old_item.bucket_index] = child_off;
          log_debug("update bucket index to: %d", child_off);
        }

        fdb_item fitem;
        memset(&fitem, 0, sizeof(fitem));
        fitem.meta_offset = old_item.meta_offset;
        fitem.meta.size = 0;
        {
          // protect the dbHeader
          tbsys::CThreadGuard guard(&header_lock);
          if(header->free_idx_head != 0) {
            fitem.meta.left = header->free_idx_head;
            log_debug("set free index head, old: %llu, new: %u",
                      header->free_idx_head, old_item.meta_offset);
          }
          else {
            log_debug("set free index head to %u", old_item.meta_offset);
          }
          header->free_idx_head = old_item.meta_offset;
        }
        write_meta(fitem);
      }

      void fdb_item_manager::free_data(fdb_item & old_item)
      {
        free_block free_block;
        free_block.size = old_item.meta.size;
        free_block.offset = old_item.meta.offset;
        fb_manager->insert(free_block);

        old_item.data.flag = TAIR_ITEM_FLAG_DELETED;
        data_file->pwrite(&old_item.data, FDB_ITEM_DATA_SIZE,
                          old_item.meta.offset);
      }

      void fdb_item_manager::new_item(fdb_item & ret_item)
      {
        tbsys::CThreadGuard guard(&header_lock);
        // find meta room
        ret_item.meta_offset = 0;
        PROFILER_BEGIN("search freeindex");
        if(header->free_idx_head != 0) {
          fdb_item temp_item;
          temp_item.meta_offset = header->free_idx_head;
          log_debug("read from freeindex, offset: %llu",
                    temp_item.meta_offset);
          if(read_meta(temp_item) && (temp_item.meta.size == 0)) {
            log_debug("set new item's meta offset to: %llu",
                      temp_item.meta_offset);
            ret_item.meta_offset = temp_item.meta_offset;
            header->free_idx_head = temp_item.meta.left;
          }
        }
        PROFILER_END();

        if(ret_item.meta_offset == 0) {
          ret_item.meta_offset = header->idx_file_size;
          log_debug("set meta offset to index file: %llu",
                    header->idx_file_size);
          header->idx_file_size += FDB_ITEM_META_SIZE;
        }

        new_data(ret_item, false);
        ret_item.log_self("new Item return: ");
      }

      void fdb_item_manager::new_data(fdb_item & ret_item, bool exclusive)
      {
        // find data room
        if(exclusive)
          tbsys::CThreadGuard guard(&header_lock);

        free_block fb;
        PROFILER_BEGIN("search free data block");
        if(fb_manager->search(ret_item.meta.size, fb)) {
          ret_item.meta.offset = fb.offset;
          ret_item.meta.size = fb.size;
        }
        else {
          ret_item.meta.offset = header->dat_file_size;
          header->dat_file_size += ret_item.meta.size;
        }
        PROFILER_END();
      }

      bool fdb_item_manager::read_meta(fdb_item & ret_item)
      {
        bool rc =
          index_file->pread((char *) &ret_item.meta, FDB_ITEM_META_SIZE,
                            ret_item.meta_offset);
#ifdef TAIR_DEBUG
        if(rc) {
          log_debug("read meta successed, offset: %d", ret_item.meta_offset);
          ret_item.log_self("read meta result: ");
        }
        else {
          log_debug("read meta failed, offset: %d", ret_item.meta_offset);
        }
#endif
        return rc;
      }

      bool fdb_item_manager::write_meta(fdb_item & old_item)
      {
        assert(old_item.meta_offset > 0);
        tbsys::CThreadGuard guard(&header_lock);

        bool rc =
          index_file->pwrite((char *) &old_item.meta, FDB_ITEM_META_SIZE,
                             old_item.meta_offset);
        if(rc && old_item.is_new) {
          if(old_item.parent_off > 0) {
            fdb_item parent;
            parent.meta_offset = old_item.parent_off;
            if(!read_meta(parent)) {
              rc = false;
            }
            else {
              if(old_item.is_right)
                parent.meta.right = old_item.meta_offset;
              else
                parent.meta.left = old_item.meta_offset;
              rc =
                index_file->pwrite((char *) &parent.meta, FDB_ITEM_META_SIZE,
                                   old_item.parent_off);
              log_debug("update parent %d", old_item.parent_off);
            }
          }
          else {
            buckets[old_item.bucket_index] = old_item.meta_offset;
            log_debug("update bucket[%d] index to: %d", old_item.bucket_index,
                      old_item.meta_offset);
          }
        }
        return rc;
      }

      bool fdb_item_manager::read_data(fdb_item & ret_item, char *data,
                                       int read_type)
      {
        size_t size = 0;
        off_t offset = 0;
        switch (read_type) {
        case READ_DATA_META:
          size = FDB_ITEM_DATA_SIZE;
          offset = ret_item.meta.offset;
          break;
        case READ_DATA_KEY:
          size = ret_item.data.keysize;
          offset = ret_item.meta.offset + FDB_ITEM_DATA_SIZE;
          break;
        case READ_DATA_VALUE:
          size = ret_item.data.valsize;
          offset =
            ret_item.meta.offset + FDB_ITEM_DATA_SIZE + ret_item.data.keysize;
          break;
        }

        assert(size != 0);

        if(!data_file->pread(data, size, offset)) {
          return false;
        }
        if(read_type == READ_DATA_META) {
          ret_item.data = *((item_meta_info *) data);
          ret_item.data.log_self();
        }
        return true;
      }

      bool fdb_item_manager::write_data(fdb_item & old_item)
      {
        assert(old_item.meta.offset > 0);
        old_item.data.log_self();
        old_item.data.magic = TAIR_DATA_MAGIC_NUMBER;
        old_item.data.checksum = 0;
        char buffer[old_item.meta.size];
        memset(buffer, 0, old_item.meta.size);
        char *bp = buffer;
        memcpy(bp, &old_item.data, FDB_ITEM_DATA_SIZE);
        bp += FDB_ITEM_DATA_SIZE;
        memcpy(bp, old_item.key, old_item.data.keysize);
        bp += old_item.data.keysize;
        memcpy(bp, old_item.value, old_item.data.valsize);

        bool rc =
          data_file->pwrite(buffer, old_item.meta.size, old_item.meta.offset);
        return rc;
      }

      void fdb_item_manager::stat_add(int area, int data_size, int use_size)
      {
        assert(area < TAIR_MAX_AREA_COUNT);
        tbsys::CThreadGuard guard(&header_lock);
        header->stat[area].add_data_size(data_size);
        header->stat[area].add_use_size(use_size);
        header->stat[area].add_item_count();
      }

      void fdb_item_manager::stat_sub(int area, int data_size, int use_size)
      {
        assert(area < TAIR_MAX_AREA_COUNT);
        tbsys::CThreadGuard guard(&header_lock);
        header->stat[area].sub_data_size(data_size);
        header->stat[area].sub_use_size(use_size);
        header->stat[area].sub_item_count();
      }

      void fdb_item_manager::backup()
      {
        char surfix[256];
        time_t now;
        time(&now);
        struct tm *ts =::localtime((const time_t *) &now);
        sprintf(surfix, "bakup.%04d%02d%02d%02d%02d%02d", ts->tm_year + 1900,
                ts->tm_mon + 1, ts->tm_mday, ts->tm_hour, ts->tm_min,
                ts->tm_sec);

        index_file->append_name(surfix);
        data_file->append_name(surfix);
      }

      tair_pstat *fdb_item_manager::get_stat() const
      {
        return header->stat;
      }

    }                                /* fdb */
  }                                /* storage */
}                                /* tair */
