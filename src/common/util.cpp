/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * some commonly used util class
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "util.hpp"
#include <vector>
#include <string>

namespace tair {
  namespace util {
    uint64_t local_server_ip::ip;

    int file_util::change_conf(const char *group_file_name, const char *section_name, const char *key, const char *value) {
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
      key_value.reserve(strlen(key) + strlen(value) + 3); //~ plus '=', '\n' and '\0'
      key_value += key;
      key_value += "=";
      key_value += value;
      key_value += "\n";
      //~ searching for the specific _group_status
      size_t k = i;
      for (; k < j; ++k) {
        const char *p = lines[k].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
          continue;
        if ((p = strstr(p, key)) != NULL) {
          p += strlen(key);
          while (*p && (*p == ' ' || *p == '\t')) ++p;
          if (*p == '=') { //~ regarding prefix
            lines[k].swap(key_value);
            break;
          }
        }
      }
      if (k == j) {
        lines.insert(lines.begin() + i, key_value);
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
  }
}
