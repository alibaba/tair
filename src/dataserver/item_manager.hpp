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
#ifndef __JSON_H
#define __JSON_H

#include <boost/any.hpp>
#include "data_entry.hpp"
#include "tair_manager.hpp"
#include "tinyjson.hpp"
#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>

namespace tair {
   namespace json {

#define VAL_COUNT(x) (((x) & 0xffff))
#define VAL_TYPE(x) ((((x) >> 16) & 0x7))
#define SET_VAL_COUNT(v,x) ( (v) |= ((x) & 0xffff))
#define SET_VAL_TYPE(v,x) ( (v)|= (((x) & 0x7) << 16))

      class item_manager {
      public:
         static const int MAX_ITEMS = 65535;
         static int add_items(tair_manager *tair_mgr,
                              int area,
                              data_entry& key,
                              data_entry& value,
                              int max_count,
                              int expired = 0);

         static int get_items(tair_manager *tair_mgr,
                              int area,
                              data_entry& key,
                              int offset,
                              int count,
                              data_entry& value /*out*/,
                              int type = ELEMENT_TYPE_INVALID);

         static int get_item_count(tair_manager *tair_mgr,
                                   int area,
                                   data_entry& key);

         static int remove_items(tair_manager *tair_mgr,
                                 int area,
                                 data_entry& key,
                                 int offset,
                                 int count,
                                 data_entry* d_value = 0,
                                 int e_type = ELEMENT_TYPE_INVALID);

         static int get_and_remove(tair_manager *tair_mgr,
                                   int area,
                                   data_entry& key,
                                   int offset,
                                   int count,
                                   data_entry& value /*out*/,
                                   int type = ELEMENT_TYPE_INVALID);

         static int new_items(const data_entry& value,
                              int type,
                              int max_count,
                              string& result);

         static int merge_items(const data_entry& old_value,
                                const data_entry& new_value,
                                int type,int max_count,
                                std::string& result);

         static void add_esc_char(std::string& src,std::string& dest){
            for(std::string::iterator ch=src.begin(); ch != src.end();++ch){
               switch( *ch )
               {
                  default: dest += *ch; break;
                  case '"': dest += "\\\""; break;
                  case '\\':dest += "\\\\"; break;
                  case '\b':dest += "\\b" ; break;
                  case '\f':dest += "\\f" ; break;
                  case '\n':dest += "\\n" ; break;
                  case '\r':dest += "\\r" ; break;
                  case '\t':dest += "\\t" ; break;
               }
            }

         }

         template<typename IT>
         static bool prepare_serialize(int type,
                                       const IT& start,
                                       const IT& end,
                                       std::string& result,
                                       bool only_value= false);

         template<typename T,typename IT,typename ELEMENT_TYPE>
         static bool serialize(const IT& start,
                               const IT& end,
                               std::string& result,
                               T type,
                               ELEMENT_TYPE& element_type);

         template<typename T,typename IT>
         static bool serialize(const IT& start,
                               const IT& end,
                               std::string& result,
                               T type,
                               boost::any& elemnet_type);

         template<typename IT,typename ELEMENT_TYPE>
         static bool serialize(const IT& start,
                               const IT& end,
                               std::string& result,
                               std::string& /*type */,
                               ELEMENT_TYPE& /*element_type*/);

         template<typename IT>
         static bool serialize(const IT& start,
                               const IT& end,
                               std::string& result,
                               std::string& /*type*/,
                               boost::any& /*element_type*/);

         template<typename IT>
         static bool serialize(const IT& start,
                               const IT& end,
                               std::string& result,
                               double /*type*/,
                               boost::any& /*elemnet type*/);


         template<typename IT,typename Type,template <typename T,typename = std::allocator<T> >class CONT  >
         static bool parse_array(const IT& start,
                                 const IT& end,
                                 CONT<Type>& result);

         template<typename IT>
         static bool parse_array(const IT& start,
                                 const IT& end,
                                 std::vector<boost::any>& result);

         template<typename IT, template <typename T,typename = std::allocator<T> > class CONT >
         static bool parse_array(const IT& start,
                                 const IT& end,
                                 CONT<double>& result);


         static bool parse_array(const std::string& src,
                                 std::vector<boost::any>& result){

            json::grammar<char>::variant var = json::parse(src.begin(),src.end());

            if(var->type() != typeid(json::grammar<char>::array)){
               cout << "parse failed.\n";
               return false;
            }
            try {
               json::grammar<char>::array const &obj = boost::any_cast<json::grammar<char>::array>(*var);
               for(json::grammar<char>::array::const_iterator it=obj.begin();it != obj.end();++it){
                  result.push_back((*(*it)));
               }
            }catch(boost::bad_any_cast& e){
               return false;
            }

            return true;

         }


         static uint32_t get_attribute(const data_entry& value){
            return ntohl(( *( reinterpret_cast<uint32_t *>( value.get_data() ) ) ));
         }

         static void set_attribute(data_entry& value,uint32_t attr){
            ( *( reinterpret_cast<uint32_t *>( value.get_data() ) ) ) = htonl(attr);
         }

         static char* get_items_addr(const data_entry& value){
            return value.get_data() + sizeof(uint32_t);
         }
         static int get_items_len(const data_entry& value){
            return value.get_size() - sizeof(uint32_t);
         }

         static int
         __get_items(tair_manager *tairManager,
                     int area,
                     data_entry& key,
                     data_entry& value, /*out*/
                     std::vector<boost::any>& items /*out*/,
                     int type = ELEMENT_TYPE_INVALID);

         static int
         __set_region(std::vector<boost::any>& items,
                      int offset,int count,
                      std::vector<boost::any>::iterator& start,
                      std::vector<boost::any>::iterator& end);

      };

      template<typename IT>
      bool item_manager::prepare_serialize(int type,const IT& start,const IT& end,std::string& result,bool only_value /*= false*/)
      {
         //needn't check arg again
         log_debug("start prepare_serialize,type:%d",type);
         if( !only_value ){
            result += "[";
         }
         bool ret = false;
         switch(type){
            case 0: //int (32bit) fall through
               //ret = serialize(start,end,result,0,*start);
               //break;
            case 1: {//long long (64bit)
               int64_t type = 0;
               ret = serialize(start,end,result,type,*start);
               break;
            }
            case 2: //double
               ret = serialize(start,end,result,0.0,*start);
               break;
            case 3: { //string
               std::string s("type");
               ret = serialize(start,end,result,s,*start);
               break;
            }
            default:
               break;
         }
         if( !only_value ){
            result += "]";
         }
         log_debug("end prepare_serialize,data:%s",result.c_str() + 4);
         return ret;
      }

/**
 * @brief convert object[] to json,we *only* support object[]
 *
 * @param start
 * @param end
 * @param result [out]
 * @param type
 *
 * @return
 */

      template<typename T,typename IT,typename ELEMENT_T>
      bool item_manager::serialize(const IT& start,const IT& end,std::string& result, T type,ELEMENT_T& /*element type*/)
      {
         std::string tmp_result;
         try {
            //there is no comma after the last element.
            tmp_result += boost::lexical_cast<std::string>( (*start) ) ;

            IT it = start;
            advance(it,1);
            for(;it != end;++it){
               tmp_result += "," + boost::lexical_cast<std::string>( (*it) );
            }
         }catch(boost::bad_lexical_cast& e){
            return false;
         }
         //add_esc_char(tmp_result,result);
         result += tmp_result;
         return true;
      }


      template<typename T,typename IT>
      bool item_manager::serialize(const IT& start,const IT& end,std::string& result,T type,boost::any& /*elemnet type*/)
      {
         std::string tmp_result;
         try {
            //there is no comma after the last element.
            tmp_result += boost::lexical_cast<std::string>( boost::any_cast<T>(*(start)) );
            IT it = start;
            advance(it,1);
            for(;it != end;++it){
               tmp_result += "," + boost::lexical_cast<std::string>( boost::any_cast<T>(*it));
            }
         }catch(boost::bad_lexical_cast& e){
            cerr << e.what() << endl;
            return false;
         }catch(boost::bad_any_cast& e){
            cerr << e.what() << endl;
            return false;
         }
         //add_esc_char(tmp_result,result);
         result += tmp_result;
         return true;
      }

/**
 * @brief specialize for string
 *
 * @param start
 * @param end
 * @param result [out]
 * @param
 *
 * @return
 */
      template<typename IT,typename ELEMENT_TYPE>
      bool item_manager::serialize(const IT& start,const IT& end,std::string& result,std::string& /*type */,ELEMENT_TYPE& /*element type*/)
      {
         std::string tmp_result;
         std::string item;
         std::string quote("\"");
         try {

            item = boost::lexical_cast<std::string>(*start);
            add_esc_char(item,tmp_result);

            result += quote + tmp_result  + quote;
            tmp_result.clear();

            IT it = start;
            advance(it,1);
            for(;it != end;++it){
               item = boost::lexical_cast<std::string>(*it);
               add_esc_char(item,tmp_result);

               result += ","  + quote + tmp_result  + quote;
               tmp_result.clear();
            }
         }catch(boost::bad_lexical_cast& e){
            return false;
         }
         return true;
      }

      template<typename IT>
      bool item_manager::serialize(const IT& start,
                                   const IT& end,
                                   std::string& result,
                                   std::string& /*type */,
                                   boost::any& /*element type*/)
      {
         std::string tmp_result;
         std::string item;
         std::string quote("\"");
         try {

            item = boost::any_cast<std::string>(*start);
            add_esc_char(item,tmp_result);

            result += quote + tmp_result  + quote;
            tmp_result.clear();

            IT it = start;
            advance(it,1);
            for(;it != end;++it){

               item = boost::any_cast<std::string>(*it);
               add_esc_char(item,tmp_result);

               result += ","  + quote + tmp_result  + quote;
               tmp_result.clear();
            }
         }catch(boost::bad_lexical_cast& e){
            return false;
         }catch(boost::bad_any_cast& e){
            return false;
         }
         return true;
      }

/**
 * @brief specialize for (double & any)
 *
 * @param start
 * @param end
 * @param result [out]
 * @param type
 *
 * @return
 */

      template<typename IT>
      bool item_manager::serialize(const IT& start,const IT& end,std::string& result,double /*type*/,boost::any& /*elemnet type*/)
      {
         std::string tmp_result;
         try {
            //there is no comma after the last element.
            double *d = 0;
            int64_t *dt = 0;
            d = boost::any_cast<double> ( &(*start)); //TODO
            if( d == 0){
               dt = boost::any_cast<int64_t> ( &(*start));
               if(dt == 0){ //
                  return false;
               }
            }
            tmp_result += boost::lexical_cast<std::string>( d ? *d : *dt );
            IT it = start;
            advance(it,1);
            d = 0;
            dt = 0;
            for(;it != end;++it){

               d = boost::any_cast<double> ( &(*it));
               if( d == 0){
                  dt = boost::any_cast<int64_t>( &(*it));
                  if(dt == 0){
                     return false;
                  }
               }
               tmp_result += "," + boost::lexical_cast<std::string>( d ? *d : *dt);

               d = 0;
               dt = 0;
            }
         }catch(boost::bad_lexical_cast& e){
            return false;
         }catch(boost::bad_any_cast& e){
            return false;
         }
         result += tmp_result;
         return true;
      }


      template<typename IT,typename Type,
               template <typename T,typename = std::allocator<T> > class CONT >
      bool item_manager::parse_array(const IT& start,
                                     const IT& end,
                                     CONT<Type>& result)
      {
         json::grammar<char>::variant var = json::parse(start,end);

         if(var->type() != typeid(json::grammar<char>::array)){
            cerr << "parse failed.\n";
            return false;
         }
         try {
            json::grammar<char>::array const &obj = boost::any_cast<json::grammar<char>::array>(*var);
            for(json::grammar<char>::array::const_iterator it=obj.begin();it != obj.end();++it){
               result.push_back( boost::any_cast<Type>(*(*it)));
            }
         }catch(boost::bad_any_cast& e){
            return false;
         }
         return true;

      }

      template<typename IT, template <typename T,typename = std::allocator<T> > class CONT >
      bool item_manager::parse_array(const IT& start,
                                     const IT& end,
                                     CONT<double>& result)
      {
         json::grammar<char>::variant var = json::parse(start,end);

         if(var->type() != typeid(json::grammar<char>::array)){
            cerr << "parse failed.\n";
            return false;
         }
         try {
            json::grammar<char>::array const &obj = boost::any_cast<json::grammar<char>::array>(*var);
            for(json::grammar<char>::array::const_iterator it=obj.begin();it != obj.end();++it){
               double *d = boost::any_cast<double>( &(*(*it)) );
               int64_t *dt = 0;
               if(d == 0){
                  dt = boost::any_cast<int64_t>( &(*(*it)) );
                  if(dt == 0)
                     return false;

               }
               result.push_back( d ? *d : *dt);
            }
         }catch(boost::bad_any_cast& e){
            cerr << e.what() << endl;
            return false;
         }
         return true;

      }

      template<typename IT>
      bool item_manager::parse_array(const IT& start,const IT& end,std::vector<boost::any>& result)
      {
         json::grammar<char>::variant var = json::parse(start,end);

         if(var->type() != typeid(json::grammar<char>::array)){
            cout << "parse failed.\n";
            return false;
         }
         try {
            json::grammar<char>::array const &obj = boost::any_cast<json::grammar<char>::array>(*var);
            for(json::grammar<char>::array::const_iterator it=obj.begin();it != obj.end();++it){
               result.push_back((*(*it)));
            }
         }catch(boost::bad_any_cast& e){
            return false;
         }
         return true;
      }

   } /* json */

} /* tair */
#endif /*__JSON_H*/
