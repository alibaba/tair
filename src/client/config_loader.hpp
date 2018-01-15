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

#ifndef __CONFIG_LOADER__
#define __CONFIG_LOADER__

#include <stdio.h>
#include <stdlib.h>

#define CONFIG_BUFFER_SIZE 4096

class ConfigLoader {
public:
    ConfigLoader();

    ~ConfigLoader();

    const char *getMsConfigServ();

    const char *getSlConfigServ();

    const char *getGroupName();

    const int getNameSpace();

    const char *getKeyFile();

    const int getWorkerNum();

    const int getDelJobSize();

    const int getKeyFormat();

    int loadInfo(const char *filename);

private:
    char *match(char *src, const char *matcher);

    int isNote(const char *notes);

private:
#define CONFIG_SERVER_1 "CONFIG_SERVER_1"

    char *_master_config_server;
#define CONFIG_SERVER_2 "CONFIG_SERVER_2"

    char *_slave_config_server;
#define GROUP_NAME "GROUP_NAME"

    char *_group_name;
#define NAMESPACE "NAMESPACE"

    int _namespace;
#define KEYS_FILE "KEYS_FILE"

    char *_keys_file;
#define WORKER_NUM "WORKER_NUM"

    int _worker_num;
#define DEL_JOB_SIZE "DEL_JOB_SIZE"

    int _del_job_size;
#define KEY_FORMAT "KEY_FORMAT"

    int _key_format;
};

ConfigLoader::ConfigLoader() {
    _master_config_server = NULL;
    _slave_config_server = NULL;
    _group_name = NULL;
    _namespace = 0;
    _keys_file = NULL;
    _worker_num = 1;
    _del_job_size = 10;
    _key_format = 0;
}

ConfigLoader::~ConfigLoader() {
    if (_master_config_server != NULL) {
        free(_master_config_server);
    }
    if (_slave_config_server != NULL) {
        free(_slave_config_server);
    }
    if (_group_name != NULL) {
        free(_group_name);
    }
    if (_keys_file != NULL) {
        free(_keys_file);
    }
}

const char *ConfigLoader::getMsConfigServ() {
    return _master_config_server;
}

const char *ConfigLoader::getSlConfigServ() {
    return _slave_config_server;
}

const char *ConfigLoader::getGroupName() {
    return _group_name;
}

const int ConfigLoader::getNameSpace() {
    return _namespace;
}

const char *ConfigLoader::getKeyFile() {
    return _keys_file;
}

const int ConfigLoader::getWorkerNum() {
    return _worker_num;
}

const int ConfigLoader::getDelJobSize() {
    return _del_job_size;
}

const int ConfigLoader::getKeyFormat() {
    return _key_format;
}

int ConfigLoader::loadInfo(const char *tool_file) {
    FILE *fp = fopen(tool_file, "r");
    if (fp == NULL) {
        fprintf(stderr, "can't open file\n");
        return 0;
    }

    const char *value = NULL;
    char buffer[CONFIG_BUFFER_SIZE];
    while (fgets(buffer, 4095, fp) != NULL) {
        if (isNote(buffer)) {
            continue;
        }

        value = match(buffer, CONFIG_SERVER_1);
        if (value != NULL) {
            int len = strlen(value);
            _master_config_server = (char *) malloc(sizeof(char) * (len + 1));
            if (_master_config_server == NULL) {
                fprintf(stderr, "file %s, line %d: malloc failed\n", __FILE__, __LINE__);
            }
            strcpy(_master_config_server, value);
            continue;
        }

        value = match(buffer, CONFIG_SERVER_2);
        if (value != NULL) {
            int len = strlen(value);
            _slave_config_server = (char *) malloc(sizeof(char) * (len + 1));
            if (_slave_config_server == NULL) {
                fprintf(stderr, "file %s, line %d: malloc failed\n", __FILE__, __LINE__);
            }
            strcpy(_slave_config_server, value);
            continue;
        }

        value = match(buffer, GROUP_NAME);
        if (value != NULL) {
            int len = strlen(value);
            _group_name = (char *) malloc(sizeof(char) * (len + 1));
            if (_group_name == NULL) {
                fprintf(stderr, "file %s, line %d: malloc failed\n", __FILE__, __LINE__);
            }
            strcpy(_group_name, value);
            continue;
        }

        value = match(buffer, NAMESPACE);
        if (value != NULL) {
            sscanf(value, "%d", &_namespace);
            continue;
        }

        value = match(buffer, KEYS_FILE);
        if (value != NULL) {
            int len = strlen(value);
            _keys_file = (char *) malloc(sizeof(char) * (len + 1));
            if (_keys_file == NULL) {
                fprintf(stderr, "file %s, line %d: malloc failed\n", __FILE__, __LINE__);
            }
            strcpy(_keys_file, value);
            continue;
        }

        value = match(buffer, WORKER_NUM);
        if (value != NULL) {
            sscanf(value, "%d", &_worker_num);
            continue;
        }

        value = match(buffer, DEL_JOB_SIZE);
        if (value != NULL) {
            sscanf(value, "%d", &_del_job_size);
            continue;
        }

        value = match(buffer, KEY_FORMAT);
        if (value != NULL) {
            sscanf(value, "%d", &_key_format);
            continue;
        }
    }

    if (fp != NULL) {
        fclose(fp);
    }

    return 1;
}

char *ConfigLoader::match(char *src, const char *matcher) {
    if (src == NULL || matcher == NULL) {
        return 0;
    }
    int len_src = strlen(src);
    int len_matcher = strlen(matcher);

    while (len_src > 0) {
        if (src[len_src - 1] == ' ' || src[len_src - 1] == '\n') {
            src[len_src - 1] = '\0';
            len_src--;
            continue;
        }
        break;
    }

    int index = 0;
    for (; index < len_src; index++) {
        if (src[index] == ' ' || src[index] == '\n') {
            continue;
        }
        break;
    }
    if (index >= len_src) {
        return NULL;
    }

    int i;
    for (i = index; i < len_src && i < len_matcher; i++) {
        if (src[i] != matcher[i]) {
            break;
        }
    }
    if (i != len_matcher) {
        return 0;
    }

    index = i;
    for (; index < len_src; index++) {
        if (src[index] == ' ' || src[index] == '\n') {
            continue;
        }
        break;
    }
    if (index >= len_src) {
        return NULL;
    }

    if (src[index++] != '=') {
        return NULL;
    }

    for (; index < len_src; index++) {
        if (src[index] == ' ' || src[index] == 0) {
            continue;
        }
        break;
    }
    if (index >= len_src) {
        return NULL;
    }

    return src + index;
}

int ConfigLoader::isNote(const char *note) {
    int len = strlen(note);
    int i;
    for (i = 0; i < len; i++) {
        if (note[i] == ' ' || note[i] == '\n') {
            break;
        }
        if (note[i] == '#') {
            return 1;
        }
    }
    if (i == len) {
        return 1;
    }
    return 0;
}

#endif
