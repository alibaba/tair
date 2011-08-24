/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#include <vector>
#include <string>
#include <map>
#include <set>
#include <utility>
#include <algorithm>
#include <string>
#include "define.hpp"
#include "conf_server_table_manager.hpp"

using namespace tair;
using namespace tair::config_server;

typedef pair<uint64_t, uint32_t> server_id_type;
typedef vector<server_id_type> hash_table_line_type;
typedef map<int, hash_table_line_type> hash_table_type;
hash_table_type hash_table;
hash_table_type m_hash_table;
hash_table_type d_hash_table;

typedef vector<int> buckets_list_type;
typedef map<uint64_t, buckets_list_type> ds_bucket_map;
ds_bucket_map ds_bucket;

vector<uint64_t> ds_list;

uint32_t copy_count;
uint32_t bucket_count;
uint32_t pos_mask;

void load_hash_table(hash_table_type & hash_table_data, uint64_t * p_hash_table)
{
  hash_table_data.clear();
  for(uint32_t i = 0; i < copy_count; i++) {
    hash_table_data[i].reserve(bucket_count);
    for(uint32_t j = 0; j < bucket_count; j++) {
      hash_table_data[i].
        push_back(make_pair(*p_hash_table, (*p_hash_table) & pos_mask));
      p_hash_table++;
    }
  }
}

void build_ds_bucket_structs(){
  ds_bucket.clear();
  for(uint32_t i = 0; i < copy_count; i++) {
    for(uint32_t j = 0; j < bucket_count; j++) {
      ds_bucket_map::iterator iter;
      if((iter=ds_bucket.find(hash_table[i][j].first))
          !=ds_bucket.end()){
        iter->second.push_back(j);
      } else {
        buckets_list_type _bl;
        _bl.clear();
        _bl.push_back(j);
        ds_bucket.insert(make_pair(hash_table[i][j].first,_bl));
      }
    }
  }
}

int caculate_ds_count(hash_table_type & hash_table){
  ds_list.clear();
  for(uint32_t j = 0; j < bucket_count; ++j) {
    for(uint32_t i = 0; i < copy_count; ++i) {
      bool flag = false;
      for(size_t k=0; k < ds_list.size(); k++){
        if(ds_list[k]==hash_table[i][j].first){
          flag = true;
          break ;
        }
      }
      if(flag==false) ds_list.push_back(hash_table[i][j].first);
    }
  }
  return  ds_list.size();
}


void print_hash_table_order_by_bucket(hash_table_type & hash_table){
  for(uint32_t j = 0; j < bucket_count; ++j) {
    char kk[64];
    sprintf(kk, "%u-->", j);
    string ss(kk);
    for(uint32_t i = 0; i < copy_count; ++i) {
      char str[1024];
      sprintf(str, "%s(%-3d)  ",
          tbsys::CNetUtil::addrToString(hash_table[i][j].first).
          c_str(), hash_table[i][j].second);
      ss += str;
    }
    printf("%s\n", ss.c_str());
  }
}
void print_hash_table_order_by_ds(){
  map<uint64_t, buckets_list_type>::iterator iter;
  iter = ds_bucket.begin();
  while(iter!=ds_bucket.end()){
    char str[64];
    sprintf(str,"%s-->",tbsys::CNetUtil::addrToString(iter->first).c_str());
    string ss(str);
    for(size_t i=0;i<iter->second.size();i++){

      char bucket_id[64];
      sprintf(bucket_id,"%4d ",iter->second[i]);
      ss+=bucket_id;
    }
    printf("%s\n",ss.c_str());
    iter++;
  }
  return;
}


int main(int argc, char **argv)
{
  if(argc != 2) {
    printf("cstMonitot serverTableName\n");
    return 0;
  }
  conf_server_table_manager cstm;
  if(cstm.open(argv[1]) == false) {
    printf("can not open file %s\n", argv[1]);
    return 0;
  }
  //cstm.print_table();

  copy_count = cstm.get_copy_count();
  bucket_count = cstm.get_server_bucket_count();

  printf("loading hashtable\n");

  load_hash_table(hash_table,cstm.get_hash_table());
  load_hash_table(m_hash_table,cstm.get_m_hash_table());
  load_hash_table(d_hash_table,cstm.get_d_hash_table());

  build_ds_bucket_structs();

  printf("\n--print_hash_table_order_by_ds()--\n");
  print_hash_table_order_by_bucket(hash_table);
  printf("\n--hash_table_order_by_ds()--\n");
  print_hash_table_order_by_ds();
  //printf("buckets need migerated\n");
  //print_hash_table_order_by_ds(m_hash_table);

  //vector<>

  printf("check repeatability of buckets in all ds\n");
  int total_repetitive_error = 0;
  for(uint32_t b = 0; b < bucket_count; ++b) {
    printf("\tchecking bucket %u\n",b);
    int repetitive_error = 0;
    for(uint32_t i = 0; i < copy_count-1; ++i) {
      for(uint32_t j=i+1 ;j < copy_count; ++j){
        if(hash_table[i][b].first==hash_table[j][b].first){
          printf("\tfind same bucket in one ds\n");
          printf("\t%s(%-3d)\n",
              tbsys::CNetUtil::addrToString(hash_table[j][b].first).c_str(),
              hash_table[j][b].second);
          printf("\t%s(%-3d)\n",
              tbsys::CNetUtil::addrToString(hash_table[j][b].first).c_str(),
              hash_table[i][b].second);
          repetitive_error++;
        }
      }
    }
    printf("%s\n",
        repetitive_error==0?"\tthis bucket is not repeated":"\t\033[41;36m ERROR \033[0m this bucket is repeated");
    total_repetitive_error+=repetitive_error;
  }
  printf("%s\n",total_repetitive_error==0?"pass repeatability check":"fault in distribution");

  int ds_count = caculate_ds_count(hash_table);
  int backup_bucket = bucket_count * (copy_count - 1 );
  int ds_contain_more_bucket = backup_bucket % (ds_count - 1);
  int ds_contain_less_bucket = ds_count -1 - ds_contain_more_bucket;
  printf("there are %d dataservers\n",ds_count);
  printf("there ara %d buckets and %d copies\n",bucket_count,copy_count);
  printf("%d datasevers should contain more buckets(%d) after one crash\n",ds_contain_more_bucket,backup_bucket+1);
  printf("%d datasevers should contain less buckets(%d) after one crash\n",ds_contain_less_bucket,backup_bucket);

//equidistribution_copycount_2:
  //int total_distribution_error_count = 0 ;
  if(copy_count!=2)
    goto equidistribution_copycount_3;
  printf("check equidistribution of backup buckets in all ds copy_count = 2\n");
  {
    printf("emulate the situation when one ds is shutdown\n");
    for(size_t ds_index =0 ; ds_index<ds_list.size();ds_index++){
      uint64_t id = ds_list[ds_index];
      printf("\tassume ds %s is crashed\n",tbsys::CNetUtil::addrToString(id).c_str());


      ds_bucket_map::iterator iter=ds_bucket.find(id);

      map<uint64_t,int> ds_contain_bucket_count;
      map<uint64_t,int>::iterator dc_iter;
      ds_contain_bucket_count.clear();

      int creash_buckets_count = iter->second.size();
      int backuped_buckets_count = creash_buckets_count* (copy_count - 1 );

      int ds_contain_less_backup_bucket_count = backuped_buckets_count / (ds_count - 1);
      int ds_contain_more_backup_bucket_count = ds_contain_less_backup_bucket_count + 1;

      //int ds_contain_more_backup_bucket = backuped_buckets_count % (ds_count - 1);
      //int ds_contain_less_backup_bucket = ds_count - 1 - ds_contain_more_backup_bucket;
      printf("\t%d buckets lost, every ds should containes %d or %d buckets\n",
          creash_buckets_count,
          ds_contain_less_backup_bucket_count,
          ds_contain_more_backup_bucket_count);
      for(size_t bucket_index = 0; bucket_index< iter->second.size(); bucket_index++){
        int bucket = iter->second[bucket_index];
        for(size_t j=0 ; j<copy_count; j++){
          if(hash_table[j][bucket].first!=id){
            if((dc_iter=ds_contain_bucket_count.find(id))
                !=ds_contain_bucket_count.end()){
              //printf("(copy_count:%d,bucket:%d)%d\n",j,bucket,id);
              dc_iter->second++;
            } else {
              ds_contain_bucket_count.insert(make_pair(id,1));
            }
          }
        }
      }
      for(dc_iter=ds_contain_bucket_count.begin();
          dc_iter!=ds_contain_bucket_count.end();dc_iter++)
      {
        if(dc_iter->second!=ds_contain_less_backup_bucket_count || dc_iter->second!=ds_contain_more_backup_bucket_count)
        {
          printf("\t \033[41;36m ERROR \033[0m amount of backup bucket(%d) in ds %s\n",dc_iter->second,
              tbsys::CNetUtil::addrToString(dc_iter->first).c_str());
        }
      }
    }
  }
  return 0 ;
equidistribution_copycount_3:
  printf("check equidistribution of backup buckets in all ds copy count = 3\n");
  return 0;
}
