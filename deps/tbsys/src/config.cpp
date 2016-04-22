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

#include "config.h"
using namespace std;

namespace tbsys {
    
    static CConfig _config;

    /**
     * 构造函数
     */
    CConfig::CConfig()
    {
    }

    /**
     * 析构函数
     */
    CConfig::~CConfig()
    {
        for(STR_MAP_ITER it=m_configMap.begin(); it!=m_configMap.end(); ++it) {
            delete it->second;
        }
    }

    /**
     * 得到静态实例
     */
    CConfig& CConfig::getCConfig()
    {
        return _config;
    }

    /**
     * 解析字符串
     */
    int CConfig::parseValue(char *str, char *key, int max_key_len, char *val, int max_value_len)
    {
        int             ret = 0;
        int             snprintf_ret = 0;
        char           *p, *p1, *name, *value;
    
        if (str == NULL)
            return -1;
    
        p = str;
        // 去前置空格
        while ((*p) == ' ' || (*p) == '\t' || (*p) == '\r' || (*p) == '\n') p++;
        p1 = p + strlen(p);
        // 去后置空格
        while(p1 > p) {
            p1 --;
            if (*p1 == ' ' || *p1 == '\t' || *p1 == '\r' || *p1 == '\n') continue;
            p1 ++;
            break;
        }
        (*p1) = '\0';
        // 是注释行或空行
        if (*p == '#' || *p == '\0') return -1;
        p1 = strchr(str, '=');
        if (p1 == NULL) return -2;
        name = p;
        value = p1 + 1;
        while ((*(p1 - 1)) == ' ') p1--;
        (*p1) = '\0';
    
        while ((*value) == ' ') value++;
        p = strchr(value, '#');
        if (p == NULL) p = value + strlen(value);
        while ((*(p - 1)) <= ' ') p--;
        (*p) = '\0';
        if (name[0] == '\0')
            return -2;

        if (0 == ret)
        {
          if (max_key_len <= (snprintf_ret = snprintf(key, max_key_len, "%s", name)))
          {
            TBSYS_LOG(ERROR, "key's length (%d) is too large. max key length supported is %d. key=%s",
                strlen(name), max_key_len - 1, name);
            ret = -2;
          }
          else if (max_value_len <= (snprintf_ret = snprintf(val, max_value_len, "%s", value)))
          {
            TBSYS_LOG(ERROR, "value's length (%d) of key ('%s') is too large. max value length supported is %d. value=%s",
                strlen(value), name, max_value_len - 1, value);
            ret = -2;
          }
        }

        return ret;
    }

    int CConfig::parseLine(STR_STR_MAP *&m, char *data)
    {
        int             ret = 0;
        static const int       MAX_KEY_LENGTH = 128;
        static const int       MAX_VALUE_LENGTH = 4096;
        char            key[MAX_KEY_LENGTH], value[MAX_VALUE_LENGTH];
        char          * sName = isSectionName(data);
        // 是段名
        if (sName != NULL) {
            STR_MAP_ITER it = m_configMap.find(sName);
            if (it == m_configMap.end()) {
                m = new STR_STR_MAP();
                m_configMap.insert(STR_MAP::value_type(/*CStringUtil::strToLower(sName)*/sName, m));
            } else {
                m = it->second;
            }
        } else {
            ret = parseValue(data, key, MAX_KEY_LENGTH, value, MAX_VALUE_LENGTH);
            if (ret == -2) {
                TBSYS_LOG(ERROR, "解析错误, Line: %s", data);
            } else if (ret < 0) {
                ret = 0;
            } else if (m == NULL) {
                TBSYS_LOG(ERROR, "没在配置section, Line: %s", data);
                ret = -1;
            } else {
                STR_STR_MAP_ITER it1 = m->find(key);
                if (it1 != m->end()) {
                    it1->second += '\0';
                    it1->second += value;
                } else {
                    m->insert(STR_STR_MAP::value_type(key, value));
                }
            }
        }
        return ret;
    }

    /* 是段名 */
    char *CConfig::isSectionName(char *str) {
        if (str == NULL || strlen(str) <= 2 || (*str) != '[') 
            return NULL;
            
        char *p = str + strlen(str);
        while ((*(p-1)) == ' ' || (*(p-1)) == '\t' || (*(p-1)) == '\r' || (*(p-1)) == '\n') p--;
        if (*(p-1) != ']') return NULL;
        *(p-1) = '\0';
        
        p = str + 1;
        while(*p) {
            if ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || (*p == '_')) {
            } else {
                return NULL;
            }
            p ++;
        }
        return (str+1);
    }
    
    /**
     * 加载文件
     */
    int CConfig::load(const char *filename)
    {
        FILE           *fp;
        int             ret;
        char            data[4096];

        if ((fp = fopen(filename, "rb")) == NULL) {
            TBSYS_LOG(ERROR, "不能打开配置文件: %s", filename);
            return EXIT_FAILURE;
        }

        STR_STR_MAP *m = NULL;
        while (fgets(data, 4096, fp)) {
            ret = parseLine(m, data);
            if (ret != 0) {
                TBSYS_LOG(ERROR, "parseLine error: %s", data);
                break;
            }
        }
        fclose(fp);
        return ret;
    }

    int CConfig::loadContent(const char * content)
    {
        int             ret = 0;
        char            data[4096];

        int             content_len = strlen(content), pos = 0;

        STR_STR_MAP *m = NULL;
        if (content_len > 0) {
            do {
                ret = getLine(data, 4096, content, content_len, pos);
                if (ret != 0) {
                    TBSYS_LOG(ERROR, "getLine error: %d, %s", pos, content);
                    break;
                } else {
                    ret = parseLine(m, data);
                    if (ret != 0) {
                        TBSYS_LOG(ERROR, "parseLine error: %s", data);
                        break;
                    }
                }
            } while (pos < content_len);
        } else {
            TBSYS_LOG(INFO, "content is empty");
        }
        return ret;
    }

    int CConfig::getLine(char * buf, const int buf_len,
        const char * content, const int content_len, int & pos)
    {
        int ret = 0;
        if (pos < content_len) {
            int end = pos;
            while (end < content_len && content[end] != '\n') end++;
            if (end - pos >= buf_len) {
                TBSYS_LOG(ERROR, "line size exceed max: %d %d", end - pos, buf_len);
                ret = -1;
            } else {
                memcpy(buf, content + pos, end - pos);
                buf[end - pos] = '\0';
                if (end == content_len) {
                    pos = end;
                } else {
                    pos = end + 1;
                }
            }
        } else {
            TBSYS_LOG(ERROR, "error pos: %d", pos);
            ret = -1;
        }
        return ret;
    }

    /**
     * 取一个string
     */
    const char *CConfig::getString(const char *section, const string& key, const char *d)
    {
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return d;
         }
        STR_STR_MAP_ITER it1 = it->second->find(key);
        if (it1 == it->second->end()) {
            return d;
        }
        return it1->second.c_str();
    }
    
    /**
     * 取一string列表
     */
    vector<const char*> CConfig::getStringList(const char *section, const string& key) {
        vector<const char*> ret;
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return ret;
        }
        STR_STR_MAP_ITER it1 = it->second->find(key);
        if (it1 == it->second->end()) {
            return ret;
        }
        const char *data = it1->second.data();
        const char *p = data;
        for(int i=0; i<(int)it1->second.size(); i++) {
            if (data[i] == '\0') {
                ret.push_back(p);
                p = data+i+1;
            }
        }
        ret.push_back(p);
        return ret;
    }

    /**
     * 取一整型
     */
    int CConfig::getInt(const char *section, const string& key, int d)
    {
        const char *str = getString(section, key);
        return CStringUtil::strToInt(str, d);
    }
    
    /**
     * 取一int list
     */
    vector<int> CConfig::getIntList(const char *section, const string& key) {
        vector<int> ret;
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return ret;
        }
        STR_STR_MAP_ITER it1 = it->second->find(key);
        if (it1 == it->second->end()) {
            return ret;
        }
        const char *data = it1->second.data();
        const char *p = data;
        for(int i=0; i<(int)it1->second.size(); i++) {
            if (data[i] == '\0') {
                ret.push_back(atoi(p));
                p = data+i+1;
            }
        }
        ret.push_back(atoi(p));
        return ret;
    }
    
    // 取一section下所有的key
    int CConfig::getSectionKey(const char *section, vector<string> &keys)
    {
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return 0;
        }
        STR_STR_MAP_ITER it1;
        for(it1=it->second->begin(); it1!=it->second->end(); ++it1) {
            keys.push_back(it1->first);
        }
        return (int)keys.size();
    }
             
    // 得到所有section的名字
    int CConfig::getSectionName(vector<string> &sections)
    {
        STR_MAP_ITER it;
        for(it=m_configMap.begin(); it!=m_configMap.end(); ++it)
        {
            sections.push_back(it->first);
        }
        return (int)sections.size();
    }

    // toString
    string CConfig::toString()
    {
        string result;
        STR_MAP_ITER it;
    	STR_STR_MAP_ITER it1;
    	for(it=m_configMap.begin(); it!=m_configMap.end(); ++it) {
            result += "[" + it->first + "]\n";
    	    for(it1=it->second->begin(); it1!=it->second->end(); ++it1) {
    	        string s = it1->second.c_str();
                result += "    " + it1->first + " = " + s + "\n";
                if (s.size() != it1->second.size()) {
                    char *data = (char*)it1->second.data();
                    char *p = NULL;
                    for(int i=0; i<(int)it1->second.size(); i++) {
                        if (data[i] != '\0') continue;
                        if (p) result += "    " + it1->first + " = " + p + "\n";
    	                p = data+i+1;
    	            }
    	            if (p) result += "    " + it1->first + " = " + p + "\n";
    	        }
            }
        }
        result += "\n";    
        return result;
    }
}
///////////////
