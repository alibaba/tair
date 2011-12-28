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
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include <iostream>
#include <boost/lexical_cast.hpp>
#include <boost/any.hpp>
#include <boost/cast.hpp>

#include "tinyjson.hpp"
#include "item_manager.hpp"
#include "data_entry.hpp"

namespace tair {
   namespace json {

      using namespace std;
      using namespace boost;

      int item_manager::add_items(tair_manager *tair_mgr,int area,data_entry& key,data_entry& value,int max_count,int expired)
      {
         if( UNLIKELY(area < 0 || max_count <= 0)){
            return TAIR_RETURN_INVALID_ARGUMENT;
         }

         if( UNLIKELY(max_count > item_manager::MAX_ITEMS)){
            max_count = item_manager::MAX_ITEMS;
         }

         int version = key.get_version();
         data_entry old_value;
         log_debug("start add_items");
         if( tair_mgr->get(area,key,old_value) != TAIR_RETURN_SUCCESS){ // is new
            tair_mgr->get_storage_manager()->set_flag(value.data_meta.flag, TAIR_ITEM_FLAG_ITEM);
            // value.data_meta.flag |= TAIR_ITEM_FLAG_ITEM;
            uint32_t _new_attr = get_attribute(value);
            string _new_result(reinterpret_cast<char *>(&_new_attr),sizeof(_new_attr)); //just reserve space
            int _new_items_no = new_items(value,VAL_TYPE(_new_attr),max_count,_new_result);
            if(_new_items_no < 0){
               return TAIR_RETURN_SERIALIZE_ERROR;
            }
            SET_VAL_COUNT(_new_attr,_new_items_no);
            value.set_data(_new_result.data(),_new_result.size(),true); //we must set the value back.
            set_attribute(value,_new_attr);
            return tair_mgr->put(area,key,value,expired);
         }
         //whether the version is matched
         if( (version != 0) && (version != old_value.get_version())){
            return TAIR_RETURN_VERSION_ERROR;
         }

         //the get method will set the flag
         if( !CAN_OVERRIDE(old_value.data_meta.flag,TAIR_ITEM_FLAG_ITEM) ){
            log_debug("cann't override old value,old_value's flag:%d\n",old_value.data_meta.flag);
            return TAIR_RETURN_CANNOT_OVERRIDE;
         }

         uint8_t old_flag = 0;
         if (tair_mgr->get_storage_manager()->test_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_DELETED)) {
         // if (IS_DELETED(old_value.data_meta.flag)){ // in migrate
            old_flag = TAIR_ITEM_FLAG_DELETED;
         }

         //get and deal with attribute of old_value & value
         uint32_t old_attr = get_attribute(old_value);
         uint32_t new_attr = get_attribute(value);
         if( (VAL_TYPE(new_attr)) != VAL_TYPE(old_attr)){
            log_debug("type not match,old_attr:%u,new_attr:%u",old_attr,new_attr);
            return TAIR_RETURN_TYPE_NOT_MATCH;
         }

         //deserialize and merge old_value & value
         uint32_t attr = 0;
         string result(reinterpret_cast<char *>(&attr),sizeof(attr)); //just reserve space
         int item_num = -1;
         if( (item_num = merge_items(old_value,value,VAL_TYPE(new_attr),max_count,result)) < 0 ){
            log_debug("merge_items failed");
            return TAIR_RETURN_SERIALIZE_ERROR;
         }
         SET_VAL_COUNT(attr,item_num);
         SET_VAL_TYPE(attr,(VAL_TYPE(new_attr)));

         data_entry final_value;
         final_value.set_data(result.data(),result.size(),true);
         final_value.set_version(version);
         tair_mgr->get_storage_manager()->set_flag(final_value.data_meta.flag, TAIR_ITEM_FLAG_ITEM);
         tair_mgr->get_storage_manager()->set_flag(final_value.data_meta.flag, old_flag);

         log_debug("final_value:%s",get_items_addr(final_value));

         set_attribute(final_value,attr); //set attribute
         //put the new value into the storage & return
         return tair_mgr->put(area,key,final_value,expired);
      }


      int item_manager::get_items(tair_manager *tair_mgr,int area,data_entry& key,int offset,int count,data_entry& value /*out*/,int type)
      {
         if(area < 0 || key.get_size() <= 0 || count < 1){
            return TAIR_RETURN_INVALID_ARGUMENT;
         }
         data_entry tmp_value;
         vector<any> items;
         int ret = -1;
         log_debug("start get_items:type:%d",type);
         if (  (ret = __get_items(tair_mgr,area,key,tmp_value,items,type)) < 0){
            log_debug("__get_items failed:%d",ret);
            return ret;
         }

         vector<any>::iterator start;
         vector<any>::iterator end;

         if ( (ret = __set_region(items,offset,count,start,end)) < 0){
            log_debug("__set_region failed");
            return ret;
         }
         int result_count = end - start;
         uint32_t attr = get_attribute(tmp_value);
         uint32_t result_attr = 0;
         SET_VAL_COUNT(result_attr,result_count);
         SET_VAL_TYPE(result_attr,VAL_TYPE(attr));

         string result( reinterpret_cast<char *>(&result_attr),sizeof(uint32_t));//just reserve space
         if( !prepare_serialize(VAL_TYPE(attr),start,end,result) ){
            log_debug("prepare_serialize failed");
            return TAIR_RETURN_SERIALIZE_ERROR;
         }
         log_debug("end get_items: success");
         value.set_data(result.data(),result.size(),true);
         set_attribute(value,result_attr);
         return TAIR_RETURN_SUCCESS;
      }

      int item_manager::get_item_count(tair_manager *tair_mgr,int area,data_entry& key)
      {
         //assert(area >= 0);
         if(area < 0){
            return TAIR_RETURN_INVALID_ARGUMENT;
         }
         data_entry value;
         int ret = TAIR_RETURN_FAILED;
         if( (ret = tair_mgr->get(area,key,value)) != TAIR_RETURN_SUCCESS ){
            return ret;
         }
         if (!tair_mgr->get_storage_manager()->test_flag(value.data_meta.flag, TAIR_ITEM_FLAG_ITEM)) {
         // if( !IS_ITEM_TYPE(value.data_meta.flag) ){
            return TAIR_RETURN_TYPE_NOT_MATCH;
         }
         uint32_t attr = get_attribute(value);
         log_debug("end get_item_count, return %d", VAL_COUNT(attr));

         return VAL_COUNT(attr);
      }


      int item_manager::remove_items(tair_manager *tair_mgr,int area,data_entry& key,int offset,int count,data_entry* d_value /*= 0*/,int e_type /*=ELEMENT_INVALID_TYPE*/)
      {
//      assert(area >= 0);
//      assert(key.get_size() > 0);
//      assert(count >= 0);

         if(area < 0 || key.get_size() <= 0 || count < 1){
            return TAIR_RETURN_INVALID_ARGUMENT;
         }

         data_entry tmp_value;
         vector<any> items;
         int ret = -1;
         if (  (ret = __get_items(tair_mgr,area,key,tmp_value,items,e_type)) < 0){
            return ret;
         }

         vector<any>::iterator start;
         vector<any>::iterator end;
         if ( (ret = __set_region(items,offset,count,start,end)) < 0){
            return ret;
         }

         uint32_t attr = get_attribute(tmp_value);
         int delete_count = end - start;
         log_debug("delete_count:%d",delete_count);
         int type = VAL_TYPE(attr);

         assert(delete_count <= numeric_cast<int>(items.size()));

         if(d_value){
            uint32_t d_attr = 0;
            string d_result( reinterpret_cast<char *>(&d_attr),sizeof(d_attr)); //just reserve space
            prepare_serialize(type,start,end,d_result);
            SET_VAL_COUNT(d_attr, delete_count);
            SET_VAL_TYPE(d_attr,type);
            d_value->set_data(d_result.data(),d_result.size(),true);
            set_attribute(*d_value,d_attr); //set attribute
         }

         uint32_t result_attr = 0;
         string result( reinterpret_cast<char *>(&result_attr),sizeof(uint32_t));

         vector<any>::iterator remain_start;
         vector<any>::iterator remain_end;

         if(delete_count == numeric_cast<int>(items.size())){ //delete all
            return tair_mgr->remove(area,key);
         }else if(start == items.begin()){
            remain_start = end;
            remain_end = items.end();
            ret = prepare_serialize(type,remain_start,remain_end,result);
         }else if(end == items.end()){
            remain_start = items.begin();
            remain_end = start;
            ret = prepare_serialize(type,remain_start,remain_end,result);
         }else{
            result += "[";
            remain_start = items.begin();
            remain_end = start;
            ret = prepare_serialize(type,remain_start,remain_end,result,true);
            if(ret){
               result += ",";
               remain_start = end;
               remain_end = items.end();
               ret = prepare_serialize(type,remain_start,remain_end,result,true);
            }
            result += "]";
         }

         if( !ret ){
            return TAIR_RETURN_SERIALIZE_ERROR;
         }

         int exist_num = numeric_cast<int>(items.size()) - delete_count;
         SET_VAL_COUNT(result_attr, exist_num);
         SET_VAL_TYPE(result_attr,type);

         data_entry final_value;
         final_value.set_data(result.data(),result.size(),true);
         tair_mgr->get_storage_manager()->set_flag(final_value.data_meta.flag, TAIR_ITEM_FLAG_ITEM);
         // final_value.data_meta.flag |= TAIR_ITEM_FLAG_ITEM;
         set_attribute(final_value,result_attr);
         log_debug("remain data:%s",get_items_addr(final_value));
         return tair_mgr->put(area,key,final_value,0); //TODO expire
      }

      int item_manager::get_and_remove(tair_manager *tair_mgr,int area,data_entry& key,int offset,int count,data_entry& value,int type)
      {
         return remove_items(tair_mgr,area,key,offset,count,&value,type);
      }

      int item_manager::new_items(const data_entry& value,int type,int max_count,string& result)
      {
         vector<any> items;

         string _new(get_items_addr(value),get_items_len(value));

         log_debug("value:%s",_new.c_str());

         if( !parse_array(_new,items) ){
            return -1;
         }
         if( numeric_cast<int>(items.size()) == 0){
            //1.the data wasn't correct.
            //2.the VAL_COUNT and the real VAL_COUNT are not match.
            return -1;
         }
         int truncated = numeric_cast<int>(items.size()) -  max_count;
         if(truncated < 0){
            truncated = 0;
         }
         vector<any>::iterator start = items.begin() + truncated;
         vector<any>::iterator end = items.end();
         int item_num = end - start;
         assert(item_num > 0);
         if ( !prepare_serialize(type,start,end,result) ){
            return -1;
         }
         return  item_num;

      }

      int item_manager::merge_items(const data_entry& old_value,
                                    const data_entry& new_value,int type,int max_count,string& result)
      {
         vector<any> items;

         string _old(get_items_addr(old_value),get_items_len(old_value));
         string _new(get_items_addr(new_value),get_items_len(new_value));

         log_debug("old value:%s",_old.c_str());
         log_debug("new value:%s",_new.c_str());

         if( !parse_array(_old,items) ){
            return -1;
         }
         if( !parse_array(_new,items) ){
            return -1;
         }
         if( numeric_cast<int>(items.size()) == 0){
            //1.the data wasn't correct.
            //2.the VAL_COUNT and the real VAL_COUNT are not match.
            return -1;
         }
         int truncated = numeric_cast<int>(items.size()) -  max_count;
         if(truncated < 0){
            truncated = 0;
         }
         vector<any>::iterator start = items.begin() + truncated;
         vector<any>::iterator end = items.end();
         int item_num = end - start;
         assert(item_num > 0);
         if ( !prepare_serialize(type,start,end,result) ){
            return -1;
         }
         return  item_num;
      }


      int item_manager::__get_items(tair_manager *tair_mgr,int area,data_entry& key,data_entry& value,vector<any>& items,int type /*ELEMENT_TYPE_INVALID*/)
      {

         int ret = TAIR_RETURN_FAILED;
         if( (ret = tair_mgr->get(area,key,value)) != TAIR_RETURN_SUCCESS ){
            return ret;
         }
         if (!tair_mgr->get_storage_manager()->test_flag(value.data_meta.flag, TAIR_ITEM_FLAG_ITEM)) {
            log_debug("is not items type,flag:%d",value.data_meta.flag);
            return TAIR_RETURN_TYPE_NOT_MATCH;
         }
         if(type != ELEMENT_TYPE_INVALID){ //need check type
            uint32_t attr = get_attribute(value);
            log_debug("__get_items:type:%d,attr:%u",type,VAL_TYPE(attr));
            if(static_cast<uint32_t>(type) != VAL_TYPE(attr)){
               return TAIR_RETURN_TYPE_NOT_MATCH;
            }
         }
         if( !parse_array(get_items_addr(value),value.get_data()+value.get_size(),items)){
            log_debug("parse_array failed");
            return TAIR_RETURN_SERIALIZE_ERROR;
         }
         if(items.size() == 0){ //empty
            return TAIR_RETURN_ITEM_EMPTY;
         }
         return 0;
      }

      int item_manager::__set_region(vector<any>& items,int offset,int count,vector<any>::iterator& start,vector<any>::iterator& end)
      {
         int items_size = numeric_cast<int>(items.size()) ;
         log_debug("offset:%d,count:%d,items_size:%d",offset,count,items_size);
         if( items_size <= abs(offset) ) {
            return TAIR_RETURN_OUT_OF_RANGE;
         }

         if(count < 1){
            return TAIR_RETURN_INVALID_ARGUMENT;
         }

         if(count > items_size){
            count = items_size;
         }

         if(offset >= 0){
            start = items.begin() + offset;
         }else{
            //negative starts from -1
            vector<any>::reverse_iterator tmp_start = items.rbegin() + abs(offset);
            start = tmp_start.base();
         }

         if( (items.end() - start) > /*=*/ count ){
            end = start + count;
         }else {
            end = items.end();
         }

         return 0;
      }
   } /* json */
} /* tair */
