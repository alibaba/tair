/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * data_entry wrap item of tair, it can be key or value
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_DATA_ENTRY_HPP
#define TAIR_DATA_ENTRY_HPP
#include <set>
#include "tbsys.h"
#include "tbnet.h"
#include "util.hpp"
#include "item_data_info.hpp"
#include "define.hpp"
//#include "log.hpp"

namespace tair
{
   namespace common {
     using namespace tair::util;
     class data_entry {
     public:
       data_entry()
       {
         init();
       }
       data_entry(const data_entry &entry)
       {
         alloc = false;
         data = NULL;

         set_data(entry.data, entry.size, entry.alloc, entry.has_merged);
         prefix_size = entry.prefix_size;
         has_merged = entry.has_merged;
         has_meta_merged = entry.has_meta_merged;
         area = entry.area;
         hashcode = 0;
         server_flag = entry.server_flag;
         data_meta = entry.data_meta;
       }

       data_entry& operator=(const data_entry &entry)
       {
         if (this == &entry)
           return *this;
         set_data(entry.data, entry.size, entry.alloc, entry.has_merged);
         prefix_size = entry.prefix_size;
         has_merged = entry.has_merged;
         has_meta_merged = entry.has_meta_merged;
         area = entry.area;
         hashcode = 0;
         server_flag = entry.server_flag;
         data_meta = entry.data_meta;

         return *this;
       }

       data_entry& clone(const data_entry &entry)
       {
         assert(this != &entry);

         set_data(entry.data, entry.size, true, entry.has_merged);
         prefix_size = entry.prefix_size;
         has_merged = entry.has_merged;
         has_meta_merged = entry.has_meta_merged;
         area = entry.area;
         hashcode = 0;
         server_flag = entry.server_flag;
         data_meta = entry.data_meta;
         return *this;
       }

       bool operator==(const data_entry &entry) const
       {
         if (entry.size != size)
           return false;
         if (has_merged != entry.has_merged)
           return false;
         if (!has_merged && area != entry.area)
           return false;

         if (data == entry.data)
           return true;
         if (data == NULL || entry.data == NULL)
           return false;

         return memcmp(data, entry.data, size) == 0;
       }

       bool operator<(const data_entry &entry) const
       {
         if(size == 0 || entry.size == 0 || data == NULL || entry.data == NULL) return false;

         int min_size = (size>entry.size?entry.size:size);
         int r = memcmp(data, entry.data, min_size);
         if (r<0)
           return true;
         else if (r>0)
           return false;
         else
           return (size<entry.size);
       }

       void print_out()
       {
         if(size > 0) {
           char debug_data[4*size];
           char *d = (char *)data;
           for(int j=0; j<size; j++)
             sprintf(debug_data+3*j, "%02X ", (d[j] & 0xFF));
           //log_debug( "TairDataEntry(%d) - %s", size, debug_data);
         }
       }
       std::string get_printable_key(bool tohex=false)
       {
#define MAX_SHOW_KEY_LEN 128
         if(size <=0) { return "null key";}
         if(tohex)
         {
           int _show_size = size<MAX_SHOW_KEY_LEN?size:MAX_SHOW_KEY_LEN;
           int _need_size = _show_size*3+5;
           char *p= (char *)malloc(_need_size);
           if(!p) return "no memory for key";

           char *d = (char *)data;

           for(int j=0; j<_show_size; j++)
             sprintf(p+3*j, "%02X ", (d[j] & 0xFF));
           std::string  _print_key(p);
           free(p);
           return _print_key;
         }
         else
         {
           return std::string(m_true_data,m_true_size);
         }
       }

       data_entry(const char *str, bool alloc = true)
       {
         init();
         set_data(str, strlen(str), alloc);
       }

       data_entry(const char *data, int size, bool alloc = true)
       {
         init();
         set_data(data, size, alloc);
       }

       ~data_entry()
       {
         if (m_true_data&& alloc) {
           free(m_true_data);
         }
         //free_data();
       }

       void merge_area(int _area)
       {
         if(has_merged) {
           return;
         }
         if(size <= 0) return;
         //now should check is alloc by me. I have 2 extra head.
         if(m_true_size==size+2)
         {
           m_true_data[0]=(_area & 0xFF);
           m_true_data[1]=((_area >> 8) & 0xFF);
           size=m_true_size;
           data=m_true_data;
         }
         else
         {
           //data set by  set_alloced_data or alloc=false,should realloc
           m_true_size=size+2;
           m_true_data=(char *)malloc(m_true_size);

           //memset(m_true_data, 0, m_true_size+1);
           m_true_data[0] = (_area & 0xFF);
           m_true_data[1] = ((_area >> 8) & 0xFF);

           memcpy(m_true_data+2, data, size);

           if(alloc) {free(data);data=NULL;}
           //reset flags;
           hashcode = 0;
           data=m_true_data;
           size=m_true_size;
         }
         this->area = _area;
         has_merged = true;
       }

       int decode_area()
       {
         int target_area = area;
         if(has_merged)
         {
           assert(size > 2);
           target_area = data[1]&0xFF;
           target_area <<= 8;
           target_area |= data[0]&0xFF;

           area = target_area;
           has_merged =false;

           data=m_true_data+2;
           size=m_true_size-2;
         }
         return target_area;
       }

       void merge_meta()
       {
         if (has_meta_merged) return;

         if (size > 0) {
           int new_size = size + sizeof(item_meta_info);
           char *new_data = (char *)malloc(new_size);
           assert (new_data != NULL);

           char *p = new_data;
           memcpy(p, &data_meta, sizeof(item_meta_info));
           p += sizeof(item_meta_info);
           memcpy(p, data, size);

           //log_debug("meta merged, newsize: %d", new_size);
           set_alloced_data(new_data, new_size);
           has_meta_merged = true;
         }

       }

       void decode_meta(bool force_decode = false)
       {
         if (has_meta_merged || force_decode) {
           assert(size > (int)sizeof(item_meta_info));
           int new_size = size - sizeof(item_meta_info);
           char *new_data = (char *)malloc(new_size);
           assert (new_data != NULL);

           char *p = data;
           memcpy(&data_meta, data, sizeof(item_meta_info));
           p += sizeof(item_meta_info);
           memcpy(new_data, p, new_size);

           set_alloced_data(new_data, new_size);
           has_meta_merged = false;
         }
       }

       void set_alloced_data(const char *data, int size)
       {
         free_data();
         this->data = this->m_true_data=(char *) data;
         this->size = this->m_true_size=size;
         alloc = true;
       }

       void set_data(const char *new_data, int new_size, bool _need_alloc = true, bool _has_merged = false)
       {
         free_data();
         if(_need_alloc)
         {
           if(new_size > 0)
           {
             m_true_size= _has_merged ? new_size : new_size+2;//add area
             m_true_data=(char *)malloc(m_true_size);
             assert(m_true_data!= NULL);
             this->alloc = true;
             if (_has_merged)
             {
                data = m_true_data;
                size = m_true_size;
             } 
             else
             {
                data = m_true_data+2;
                size = m_true_size-2;
                m_true_data[0]=m_true_data[1]=0xFF;
             }

             if(new_data)
             {
               memcpy(data, new_data, size);
             }else
             {
               memset(data, 0, size);
             }
           }
         } else {
           m_true_data=data = (char *) new_data;
           m_true_size=size = new_size;
         }
       }

       inline char *get_data() const
       {
         return data;
       }

       inline char *get_prefix() const
       {
         if (prefix_size>0) 
           return has_merged ? data+2 : data;
         else 
           return NULL;
       }

       inline int get_size() const
       {
         return size;
       }

       int get_prefix_size() const
       {
         return this->prefix_size;
       }

       void set_prefix_size(int prefix_size) {
         this->prefix_size = prefix_size;
       }

       void set_version(uint16_t version)
       {
         data_meta.version = version;
       }

       uint16_t get_version() const
       {
         return data_meta.version;
       }

       uint64_t get_hashcode()
       {
         if(hashcode == 0 && size > 0 && data != NULL) {
           hashcode = hash_util::mhash1(data, size);
           hashcode <<= 32;
           hashcode |= hash_util::mhash2(data, size);
         }

         return hashcode;
       }

       uint32_t get_cdate() const
       {
         return data_meta.cdate;
       }

       void  set_cdate(uint32_t cdate)
       {
         data_meta.cdate = cdate;
       }

       uint32_t get_mdate() const
       {
         return data_meta.mdate;
       }

       void  set_mdate(uint32_t mdate)
       {
         data_meta.mdate = mdate;
       }

       inline bool is_alloc() const {
         return alloc;
       }

       size_t encoded_size() {
         return 40 + get_size();
       }

       void encode(tbnet::DataBuffer *output, bool need_compress = false) const
       {
         output->writeInt8(has_merged);
         output->writeInt32(area);
         output->writeInt16(server_flag);

#ifdef WITH_COMPRESS
         if (need_compress) {
           do_compress(output);
         } else
#endif
         {
           data_meta.encode(output);
           uint32_t msize = (size | (prefix_size << PREFIX_KEY_OFFSET));
           output->writeInt32(msize);
           if (get_size() > 0) {
             output->writeBytes(get_data(), get_size());
           }
         }
       }

       void encode_with_compress(tbnet::DataBuffer *output) const
       {
         encode(output, true);
       }

       bool decode(tbnet::DataBuffer *input, bool need_decompress = false)
       {
         free_data();
         uint8_t temp_merged = input->readInt8();
         int _area = input->readInt32();
         if(_area<0 || _area>=TAIR_MAX_AREA_COUNT) return false;
         uint16_t flag = input->readInt16();
         data_meta.decode(input);

         uint32_t msize = input->readInt32();
         size = (msize & PREFIX_KEY_MASK);
         prefix_size = (msize >> PREFIX_KEY_OFFSET);
         if (size > 0) {
           set_data(NULL, size, true, temp_merged);
           input->readBytes(get_data(), size);
         }
         bool ret = true;
#ifdef WITH_COMPRESS
         if (need_decompress) {
           ret = do_decompress();
         }
#endif
         has_merged = temp_merged;
         area = _area;
         server_flag = flag;
         return ret;
       }

       bool decode_with_decompress(tbnet::DataBuffer *input)
       {
         return decode(input, true);
       }

     private:
#ifdef WITH_COMPRESS
       void do_compress(tbnet::DataBuffer *output) const;
       bool do_decompress();
#endif

       void init()
       {
         alloc = false;
         size = m_true_size = 0;
         prefix_size = 0;
         data = m_true_data = NULL;

         has_merged = false;
         has_meta_merged = false;
         area = 0;
         hashcode = 0;
         server_flag = 0;
         memset(&data_meta, 0, sizeof(item_meta_info));
       }

       inline void free_data()
       {
         if (m_true_data&& alloc) {
           free(m_true_data);
         }
         data = NULL;
         m_true_data = NULL;
         alloc = false;
         size = m_true_size = 0;
         hashcode = 0;
         has_merged = false;
         area = 0;
       }

     private:
       int size;
       int prefix_size;
       char *data;
       bool alloc;
       uint64_t hashcode;

       int m_true_size;  //if has_merged ,or m_true_size=size+2
       char *m_true_data; //if has_merged arae,same as data,or data=m_true_data+2;
     public:
       bool has_merged;
       bool has_meta_merged;
       uint32_t area;
       uint16_t server_flag;
       item_meta_info data_meta;
#ifdef WITH_COMPRESS
       static int compress_type;
       static int compress_threshold;
#endif
     };

     class data_entry_comparator {
     public:
       bool operator() (const data_entry *a, const data_entry *b)
       {
         return ((*a) < (*b));
       }
     };

     struct data_entry_hash {
       size_t operator()(const data_entry *a) const
       {
         if(a == NULL || a->get_data() == NULL || a->get_size() == 0) return 0;

         return string_util::mur_mur_hash(a->get_data(), a->get_size());
       }
     };

     struct data_entry_equal_to {
       bool operator()(const data_entry *a, const data_entry *b) const
       {
         return *a == *b;
       }
     };


     //! Be Cautious about the return value of set::insert & hash_map::insert,
     //! which return pair<iterator, bool> type.
     //! When inserting one existing data_entry to a set or hash_map below,
     //! there is a risk of memory leaks
     typedef std::vector<data_entry *> tair_dataentry_vector;
     typedef std::set<data_entry*, data_entry_comparator> tair_dataentry_set;
     typedef __gnu_cxx::hash_map<data_entry*, data_entry*,
             data_entry_hash, data_entry_equal_to> tair_keyvalue_map;
     typedef __gnu_cxx::hash_map<data_entry*, int, data_entry_hash, data_entry_equal_to> key_code_map_t;

     class value_entry {
     public:
       value_entry() : version(0), expire(0)
       {
       }

       value_entry(const value_entry &entry)
       {
         d_entry = entry.d_entry;
         version = entry.version;
         expire = entry.expire;
       }

       value_entry& clone(const value_entry &entry)
       {
         assert(this != &entry);
         d_entry = entry.d_entry;
         version = entry.version;
         expire = entry.expire;

         return *this;
       }

       void set_d_entry(const data_entry& in_d_entry)
       {
         d_entry = in_d_entry;
       }

       data_entry& get_d_entry()
       {
         return d_entry;
       }

       void set_expire(int32_t expire_time)
       {
         expire = expire_time;
       }

       int32_t get_expire() const
       {
         return expire;
       }

       void set_version(uint16_t kv_version)
       {
         version = kv_version;
       }

       uint16_t get_version() const
       {
         return version;
       }

       void encode(tbnet::DataBuffer *output) const
       {
         d_entry.encode(output);
         output->writeInt16(version);
         output->writeInt32(expire);
       }

       void decode(tbnet::DataBuffer *input)
       {
         d_entry.decode(input);
         version = input->readInt16();
         expire = input->readInt32();
       }

       int get_size() const
       {
         return d_entry.get_size()+ 2 + 4;
       }

     private:
       data_entry d_entry;
       uint16_t version;
       int32_t expire;
     };

     class mput_record {
     public:
       mput_record()
       {
         key = NULL;
         value = NULL;
       }

       mput_record(mput_record &rec)
       {
         key = new data_entry(*(rec.key));
         value = new value_entry(*(rec.value));
       }

       ~mput_record()
       {
         if (key != NULL ) {
           delete key;
           key = NULL;
         }
         if (value != NULL) {
           delete value;
           value = NULL;
         }
       }

     public:
       data_entry* key;
       value_entry* value;
     };

     typedef std::vector<mput_record*> mput_record_vec;

     typedef __gnu_cxx::hash_map<data_entry*, value_entry*, data_entry_hash> tair_client_kv_map;


     inline void defree(tair_dataentry_vector &vector) {
       for (size_t i = 0; i < vector.size(); ++i) {
         delete vector[i];
       }
     }

     inline void defree(tair_dataentry_set &set) {
       tair_dataentry_set::iterator itr = set.begin();
       while (itr != set.end()) {
         delete *itr++;
       }
     }

     inline void defree(tair_keyvalue_map &map) {
       tair_keyvalue_map::iterator itr = map.begin();
       while (itr != map.end()) {
         data_entry *key = itr->first;
         data_entry *value = itr->second;
         ++itr;
         delete key;
         delete value;
       }
     }

     inline void defree(key_code_map_t &map) {
       key_code_map_t::iterator itr = map.begin();
       while (itr != map.end()) {
         data_entry *key = itr->first;
         ++itr;
         delete key;
       }
     }

     inline void merge_key(const data_entry &pkey, const data_entry &skey, data_entry &mkey) {
       int pkey_size = pkey.get_size();
       int skey_size = skey.get_size();
       char *buf = new char[pkey_size + skey_size];
       memcpy(buf, pkey.get_data(), pkey_size);
       memcpy(buf + pkey_size, skey.get_data(), skey_size);
       mkey.set_alloced_data(buf, pkey_size + skey_size);
       mkey.set_prefix_size(pkey_size);
     }

     inline void split_key(const data_entry *mkey, data_entry *pkey, data_entry *skey) {
       if (mkey == NULL)
         return ;
       int prefix_size = mkey->get_prefix_size();
       if (pkey != NULL) {
         pkey->set_data(mkey->get_data(), prefix_size);
       }
       if (skey != NULL) {
         skey->set_data(mkey->get_data() + prefix_size, mkey->get_size() - prefix_size);
       }
     }
   }
}

#endif
