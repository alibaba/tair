/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#ifndef TBSYS_CONFIG_H
#define TBSYS_CONFIG_H

#include <string>
#include <ext/hash_map>
#include "stringutil.h"
#include "tblog.h"

//using namespace std;
//using namespace __gnu_cxx;

namespace tbsys {
/** 
* @brief 生成string的hash值 
*/
    struct str_hash {
        size_t operator()(const std::string& str) const {
            return __gnu_cxx::__stl_hash_string(str.c_str());
        }
    };
    /** 
     * @brief 字符串比较
     */
    struct char_equal {
        bool operator()(const char* s1, const char* s2) const {
            return strcmp(s1, s2) == 0;
        }
    };
    typedef __gnu_cxx::hash_map<std::string, std::string, str_hash> STR_STR_MAP;
    typedef STR_STR_MAP::iterator STR_STR_MAP_ITER;
    typedef __gnu_cxx::hash_map<std::string, STR_STR_MAP*, str_hash> STR_MAP;
    typedef STR_MAP::iterator STR_MAP_ITER;
    
    #define TBSYS_CONFIG tbsys::CConfig::getCConfig()
    
    /** 
     * @brief 解析配置文件,并将配置项以key-value的形式存储到内存中
     */
    class           CConfig {
        public:
            CConfig();
            ~CConfig();
            
            // 加载一个文件
            int load(const char *filename);
            // 加载一个缓冲区
            int loadContent(const char * content);
            // 取一个字符串
            const char *getString(const char *section, const std::string& key, const char *d = NULL);
            // 取一string列表
            std::vector<const char*> getStringList(const char *section, const std::string& key);
            // 取一个整型
            int getInt(char const *section, const std::string& key, int d = 0);
            // 取一整型列表
            std::vector<int> getIntList(const char *section, const std::string& key);
            // 取一section下所有的key
            int getSectionKey(const char *section, std::vector<std::string> &keys);
            // 得到所有section的名字
            int getSectionName(std::vector<std::string> &sections);
            // 完整的配置文件字符串
            std::string toString();
            // 得到静态实例
            static CConfig& getCConfig();
            
        private:
            // 两层map
            STR_MAP m_configMap;
            
        private:
            // 解析字符串
            int parseValue(char *str, char *key, int max_key_len, char *val, int max_value_len);
            int parseLine(STR_STR_MAP *&m, char *data);
            int getLine(char * buf, const int buf_len,
                const char * content, const int content_len, int & pos);
            char *isSectionName(char *str);     
    };
}

#endif
