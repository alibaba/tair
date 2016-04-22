/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#include <fstream>

#include "util.hpp"

namespace tair {
namespace util {

using std::string;
using std::vector;

const unsigned char BitMap::mask_[] =
        {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

uint64_t local_server_ip::ip = 0;
Crematory *Crematory::g_crematory = new Crematory();

// change_file_type:
//      CHANGE_FILE_MODIFY: modify key->value in file
//      CHANGE_FILE_ADD:    add key->value at the end of the last duplicate key in file
//      CHANGE_FILE_DELETE: delete key->value in file
// key_delimiter:  '=' by default
int file_util::change_conf(const char *group_file_name, const char *section_name, const char *key, const char *value,
                           int change_type, char key_delimiter) {
    if (group_file_name == NULL) {
        return TAIR_RETURN_FAILED;
    }
    if (key == NULL) {
        return TAIR_RETURN_FAILED;
    }
    if (value == NULL) {
        value = "";
    }

    FILE *fd = fopen(group_file_name, "r+");
    if (fd == NULL) {
        log_error("open group file %s failed: %s", group_file_name, strerror(errno));
        return TAIR_RETURN_FAILED;
    }

    char section[128];
    snprintf(section, sizeof(section), "[%s]", section_name);

    std::vector<std::string> lines;
    lines.reserve(100);
    char line[1024]; //! assuming one line has less than 1k chars
    while (fgets(line, sizeof(line), fd) != NULL) {
        lines.push_back(line);
    }
    fclose(fd);
    //~ searching for target section
    size_t i = 0;
    for (; i < lines.size(); ++i) {
        const char *p = lines[i].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if (strstr(p, section) == NULL) {
            continue;
        }
        break;
    }
    if (i == lines.size()) {
        log_error("group file %s cann't find section: %s", group_file_name, section_name);
        return TAIR_RETURN_FAILED; //~ no such section
    }
    ++i;
    //~ searching for boundary of the next section
    size_t j = i;
    for (; j < lines.size(); ++j) {
        const char *p = lines[j].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if (*p == '[')
            break;
    }

    std::string key_value;
    key_value.reserve(strlen(key) + strlen(value) + 3); //~ plus key_delimiter, '\n' and '\0'
    key_value += key;
    key_value += key_delimiter;
    key_value += value;
    key_value += "\n";
    //~ searching for the specific _group_status
    size_t k = i;
    int last_dup_key = -1;

    for (; k < j; ++k) {
        const char *p = lines[k].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if ((p = strstr(p, key)) != NULL) {
            if (CHANGE_FILE_ADD == change_type) {
                last_dup_key = k;  // insert key at the end of the list
            } else if (CHANGE_FILE_MODIFY == change_type) {
                p += strlen(key);
                while (*p && (*p == ' ' || *p == '\t')) ++p;
                if (*p == key_delimiter) { //~ regarding prefix
                    lines[k].swap(key_value);
                    break;
                }
            } else if (CHANGE_FILE_DELETE == change_type) {
                p += strlen(key);
                while (*p && (*p == ' ' || *p == '\t')) ++p;
                if (*p == key_delimiter) {
                    lines.erase(lines.begin() + k);
                }
            }
        }
    }
    if (k == j && CHANGE_FILE_MODIFY == change_type) {
        lines.insert(lines.begin() + i, key_value);
    }
    if (CHANGE_FILE_ADD == change_type) {
        if (last_dup_key != -1) {
            lines.insert(lines.begin() + last_dup_key + 1, key_value);
        } else { // insert key at the end of the section
            lines.insert(lines.begin() + j, key_value);
        }
    }

    //~ write to temp file, then rename it
    char tmpfile[128];
    snprintf(tmpfile, sizeof(tmpfile), "%s.%d", group_file_name, getpid());
    fd = fopen(tmpfile, "w+");
    if (fd == NULL) {
        log_error("open temp file %s failed, %s", tmpfile, strerror(errno));
        return TAIR_RETURN_FAILED;
    }
    for (i = 0; i < lines.size(); ++i) {
        fprintf(fd, lines[i].c_str());
    }
    fclose(fd);
    if (0 == rename(tmpfile, group_file_name)) {
        return TAIR_RETURN_SUCCESS;
    } else {
        log_error("rename from %s to %s failed", tmpfile, group_file_name);
        return TAIR_RETURN_FAILED;
    }
}

void file_util::change_file_manipulate_kv(const string &file_name, file_util::FileUtilChangeType op_cmd,
                                          const string &section_name, char kv_delimiter,
                                          const vector<string> keys, const vector<string> values,
                                          vector<file_util::OpResult> &results) {
    if (keys.size() != values.size()) {
        results.assign(keys.size(), file_util::RETURN_KEY_VALUE_NOT_FOUND);
        return;
    }
    results.clear();
    std::ifstream ifs(file_name.c_str());
    if (ifs.is_open() == false) {
        results.assign(keys.size(), file_util::RETURN_OPEN_FILE_FAILED);
        return;
    }

    string section("[");
    section.append(section_name);
    section.append("]");

    std::vector<std::string> lines;
    lines.reserve(100);
    while (true) {
        lines.push_back("");
        std::getline(ifs, lines.back());
        // only when it reach the end and read again, get nothing, then eof is set to true
        if (ifs.eof() == true) {
            lines.pop_back();
            break;
        }
        lines.back().push_back('\n');
    }

    //~ searching for target section
    size_t section_content_begin = 0;
    for (; section_content_begin < lines.size(); ++section_content_begin) {
        const char *p = lines[section_content_begin].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if (strncmp(p, section.c_str(), section.size()) == 0)
            break;
    }
    if (section_content_begin == lines.size()) {
        results.assign(keys.size(), file_util::RETURN_SECTION_NAME_NOT_FOUND);
        return;
    }

    ++section_content_begin;
    //~ searching for boundary of the next section
    size_t section_content_end = section_content_begin;
    for (; section_content_end < lines.size(); ++section_content_end) {
        const char *p = lines[section_content_end].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if (*p == '[')
            break;
    }

    // now section content is in lines[section_content_begin, section_content_end)
    if (file_util::CHANGE_FILE_DELETE == op_cmd)
        del_kv_from_lines(keys, values, kv_delimiter, section_content_begin, section_content_end, lines, results);
    else if (file_util::CHANGE_FILE_ADD == op_cmd)
        add_kv_to_lines(keys, values, kv_delimiter, section_content_begin, section_content_end, lines, results);
    else {
        results.assign(keys.size(), file_util::RETURN_INVAL_OP_TYPE);
        return;
    }

    //~ write to temp file, then rename it
    char tmpfile[128];
    snprintf(tmpfile, sizeof(tmpfile), "%s.%d", file_name.c_str(), getpid());
    std::ofstream ofs(tmpfile);
    if (ofs.is_open() == false) {
        results.assign(keys.size(), file_util::RETURN_SAVE_FILE_FAILD);
        return;
    }
    for (size_t i = 0; i < lines.size(); ++i)
        ofs << lines[i];
    ofs.close();

    if (rename(tmpfile, file_name.c_str()) != 0)
        results.assign(keys.size(), RETURN_SAVE_FILE_FAILD);
}

void file_util::del_kv_from_lines(const vector<string> &keys, const vector<string> &values,
                                  char kv_delimiter, size_t line_begin, size_t line_end,
                                  vector<string> &lines, vector<file_util::OpResult> &results) {
    if (keys.size() != values.size()) {
        results.assign(keys.size(), file_util::RETURN_KEY_VALUE_NUM_NOT_EQUAL);
        return;
    }

    for (size_t kv_index = 0; kv_index < keys.size(); ++kv_index) {
        const char *key = keys[kv_index].c_str();
        const char *value = values[kv_index].c_str();
        const size_t key_len = keys[kv_index].size();
        const size_t value_len = values[kv_index].size();

        bool del_success = false;
        for (size_t matched_line_index = line_begin; matched_line_index < line_end; ++matched_line_index) {
            const char *p = lines[matched_line_index].c_str();
            while (*p && (*p == ' ' || *p == '\t')) ++p;
            if (*p == '#')
                continue;
            // strict match " key  =  value \n" key, value, and '\n' can have space or tab between them.
            // key match
            if (strncmp(p, key, key_len) == 0) {
                p += strlen(key);
                while (*p && (*p == ' ' || *p == '\t')) ++p;
                // delimiter exist and match
                if (*p == kv_delimiter) {
                    ++p;
                    while (*p && (*p == ' ' || *p == '\t')) ++p;
                    // value match
                    if (strncmp(p, value, value_len) == 0) {
                        p += strlen(value);
                        while (*p && (*p == ' ' || *p == '\t')) ++p;
                        // oK
                        if (*p == '\n' || *p == '#') {
                            lines[matched_line_index].clear();
                            del_success = true;
                        }
                    }
                }
            }
        } // end of for (; matched_line_index < section_content_end; ++matched_line_index)
        if (del_success)
            results.push_back(file_util::RETURN_SUCCESS);
        else
            results.push_back(file_util::RETURN_KEY_VALUE_NOT_FOUND);
    } // end of for (size_t kv_index = 0; kv_index < keys.size(); ++kv_index)
}

void file_util::add_kv_to_lines(const vector<string> &keys, const vector<string> &values,
                                char kv_delimiter, size_t line_begin, size_t line_end,
                                vector<string> &lines, vector<file_util::OpResult> &results) {
    if (keys.size() != values.size()) {
        results.assign(keys.size(), file_util::RETURN_KEY_VALUE_NOT_FOUND);
        return;
    }

    for (size_t kv_index = 0; kv_index < keys.size(); ++kv_index) {
        const char *key = keys[kv_index].c_str();
        const char *value = values[kv_index].c_str();
        const size_t key_len = keys[kv_index].size();
        const size_t value_len = values[kv_index].size();

        bool already_exist = false;
        int64_t insert_position = -1;
        for (size_t matched_line_index = line_begin; matched_line_index < line_end; ++matched_line_index) {
            const char *p = lines[matched_line_index].c_str();
            while (*p && (*p == ' ' || *p == '\t')) ++p;
            if (*p == '#')
                continue;
            // strict match " key  =  value \n" key, value, and '\n' can have space or tab between them.
            // key match
            if (strncmp(p, key, key_len) == 0) {
                p += strlen(key);
                while (*p && (*p == ' ' || *p == '\t')) ++p;
                // delimiter exist and match
                if (*p == kv_delimiter) {
                    insert_position = matched_line_index;
                    ++p;
                    while (*p && (*p == ' ' || *p == '\t')) ++p;
                    // value match
                    if (strncmp(p, value, value_len) == 0) {
                        already_exist = true;
                        break;
                    }
                }
            }
        } // end of for (; matched_line_index < section_content_end; ++matched_line_index)
        if (already_exist)
            results.push_back(file_util::RETURN_KEY_VALUE_ALREADY_EXIST);
        else {
            results.push_back(file_util::RETURN_SUCCESS);
            if (-1 == insert_position) {
                insert_position = line_end - 1;
                if (insert_position < 0)  // just in case of crash, it will not come to here
                    insert_position = 0;
            }
            if (lines[insert_position].empty() || '\n' != *(lines[insert_position].end() - 1))
                lines[insert_position].append("\n");
            lines[insert_position].append(key);
            lines[insert_position].push_back(kv_delimiter);
            lines[insert_position].append(value);
            lines[insert_position].push_back('\n');
        }
    } // end of for (size_t kv_index = 0; kv_index < keys.size(); ++kv_index)
}

}
}
