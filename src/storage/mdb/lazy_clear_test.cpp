/*
* (C) 2007-2010 Alibaba Group Holding Limited
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id: libmdb_test_c.cpp 470 2011-12-28 05:26:28Z ganyu.hfl $
*
* Authors:
*    fengmao.pj
*/
#define __STDC_FORMAT_MACROS
#include <tbsys.h>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
using namespace std;

#include "libmdb_c.hpp"

// to cache the <key,value> pairs.
class KeyValuePool{
public:
  KeyValuePool(int count):kvCount(count){
  }
  ~KeyValuePool(){
  }
public:
  void reset(){
    for(int i = 0;i < kvCount;++i){
      stringstream sk;
      sk<< i << "keys" << rand() % 1000000 << endl;
      //sk<<i<<"keys"<<i<<endl;
      keys.push_back(sk.str());

      stringstream sv;
      sv<< i << "values" << rand() % 2000000 << endl;
      //sv<<i<<"values"<<i<<endl;
      values.push_back(sv.str());

    }
  }
  char* getKey(int i){
    return (char*)keys[i].c_str();
  }

  char* getValue(int i){
    return (char*)values[i].c_str();
  }

  int inline getCount(){return kvCount;}
protected:
  vector<string> keys;
  vector<string> values;
  int kvCount;
};
int mdb_put(mdb_t db, int area, KeyValuePool& kvp, int start, int end);
int mdb_get(mdb_t db, int area, KeyValuePool& kvp, int start, int end);
void mdb_stat(mdb_t db, int area);
void mdb_lazy_clear(mdb_t db, int area);

//case 2:
void test_case_2(mdb_t db, int area, KeyValuePool& kvp, int c){

  mdb_stat(db, area);
  mdb_put(db, area, kvp, 0, c);
  mdb_stat(db, area);

  sleep(5);
  mdb_lazy_clear(db, area);
  mdb_stat(db, area);
  sleep(5);
}
void test_case_3(mdb_t db, int area, KeyValuePool& kvp, int c){
  mdb_stat(db, area);
  mdb_get(db, area, kvp, 0, c);
  mdb_stat(db, area);
}
void test_case_1(mdb_t db, int area, KeyValuePool& kvp, int c){
  mdb_stat(db, area);
  mdb_put(db, area, kvp, 0, c);
  mdb_stat(db, area);

  sleep(5);
  mdb_lazy_clear(db, area);
  mdb_stat(db, area);


  sleep(5);
  mdb_get(db, area, kvp, 0, c / 2);
  mdb_stat(db, area);

  mdb_put(db, area, kvp, c / 2, c);
  mdb_stat(db, area);
}
void test_case_4(int c){
  KeyValuePool kvp(c);
  kvp.reset();
  mdb_param_t params;
  memset(&params, 0, sizeof(mdb_param_t));
  params.mdb_type = "mdb_shm";
  params.mdb_path = "lazyClearTestPath_case4";
  params.size = 1024 * 1024 * 1024;
  mdb_t db=mdb_init(&params);
  for(int i = 0;i < 1024;++i){
    mdb_set_quota(db, i, params.size);

    int nput = mdb_put(db, i, kvp, 0, c);
    sleep(1);
    mdb_clear(db, i);
    sleep(2);
    int nget = mdb_get(db, i, kvp, 0, c);

    cout << "nput=" << nput << "  nget=" << nget << endl;
    if(nput == c && nget == 0)
      cout << "area= " << i << " --------------------------------------[OK]" << endl;
    else
      cout << "area= " << i << " -----------------------------------[ERROR]" << endl;
  }


}
int main(){
  int count_kv = 10;
  int area = 3;
  KeyValuePool kvp(count_kv);
  kvp.reset();
  //init the mdb
  mdb_param_t params;
  memset(&params, 0, sizeof(mdb_param_t));
  params.mdb_type = "mdb_shm";
  params.mdb_path = "lazyClearTestPath";
  params.size = 1024 * 1024 * 1024;

  mdb_t db=mdb_init(&params);
  //mdb_log_level("debug");
  mdb_set_quota(db, area, params.size);
//  mdb_log_file("debug_log");

  int cid;
  cout << "input the case id:";
  cin >> cid;

  switch(cid){
    case 1:
    //case 1
    test_case_1(db, area, kvp, count_kv);
    break;

    test_case_2(db, area, kvp, count_kv);
    break;

    case 3:
    test_case_3(db, area, kvp, count_kv);
    break;

    case 4:
    test_case_4(10);
    break;
    default:

    cout << "ERROR!" << endl;
  }
  return 0;
}
int mdb_put(mdb_t db, int area, KeyValuePool& kvp, int start, int end){
 int r=0;
 for(int i = start;i < end;++i){
    data_entry_t key,value;
    key.data = kvp.getKey(i);
    key.size = strlen(key.data);
    value.data = kvp.getValue(i);
    value.size = strlen(value.data);

    int rc = mdb_put(db, area, &key, &value, 0, 1, 0);
    cout << "put i=" << i << " key ";
    if(rc == 0) {
      r++;
      cout << "success " ;
    }
    else cout << "failed";
    cout << endl;
  }
return r;
}
void mdb_lazy_clear(mdb_t db, int area){
    mdb_clear(db, area);
}

int  mdb_get(mdb_t db,int area,KeyValuePool& kvp,int start,int end){
  int r=0;
  for(int i = start;i < end;++i){
    data_entry_t key,value;
    key.data = kvp.getKey(i);
    key.size = strlen(key.data);
    value.data = 0;
    value.size = 0;

    int x,y;
    int rc = mdb_get(db, area, &key, &value, &x, &y);
    cout << "get i=" << i <<" key ";
    if(rc == 0) {
      r++;
      cout << "success ";
    }
    else cout << "failed";
    cout << endl;
  }
return r;
}
void mdb_stat(mdb_t db, int area){
  mdb_stat_t stat;
  mdb_get_stat(db, area, &stat);

  cout << "AREA= "<< area << endl;
  cout << "use_size= " << stat.space_usage << endl;
  cout << "data_size= " << stat.data_size << endl;
  cout << "item_count= " << stat.item_count << endl;
  cout << "put_count= " << stat.put_count << endl;
  cout << "get_count= " << stat.get_count <<endl;
  cout << "hit_count= " << stat.hit_count <<endl;
  cout << "remove_count= " << stat.remove_count << endl;
  cout << "evict_count= " << stat.evict_count << endl;
}
