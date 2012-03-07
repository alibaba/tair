#ifndef __TAIR_KEY_CLEAN_TOOL__
#define __TAIR_KEY_CLEAN_TOOL__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <errno.h>
#include <unistd.h>

#include "DelJob.hpp"
#include "Worker.hpp"
#include "ConfigLoader.hpp"

namespace tair {
#define BUFF_SIZE 4096
#define GETS_SIZE 4095

class TairKeyCleanTool {
public:
    TairKeyCleanTool(char* configserver, char* group_id,
            int area, char* keyfile, int key_format,
            int worker_num, int key_op_max);
    ~TairKeyCleanTool();
    void execute();
    static void helper();
    void init(char* configserver, char* group_id,
            int area, char* keyfile, int key_format,
            int worker_num, int key_op_max);
private:
    void readFile();
    void postJob(DelJob* job);
private:
    char* _keyfile;
    int _worker_selected;
    Worker* _workers;
    int _worker_num;
    int _key_op_max;
};


TairKeyCleanTool::TairKeyCleanTool(char* configserver, char* group_id,
        int area, char* keyfile, int key_format, int worker_num, int key_op_max) {
    init(configserver, group_id, area, keyfile, key_format, worker_num, key_op_max);
}

void TairKeyCleanTool::init(char* configserver, char* group_id,
        int area, char* keyfile, int key_format, int worker_num, int key_op_max) {
    _worker_selected = 0;
    _worker_num = worker_num;
    _key_op_max = key_op_max;
    
    int len_keyfile = strlen(keyfile) + 1;
    _keyfile = new char[len_keyfile];
    if (_keyfile == NULL) {
        fprintf(stderr, "file %s, line %d: no enough memory\n",
                __FILE__, __LINE__);
        exit(1);
    }
    strcpy(_keyfile, keyfile);
    
    _workers = new Worker[worker_num];
    if (_workers == NULL) {
        fprintf(stderr, "file %s, line %d: no enough memory\n",
                __FILE__, __LINE__);
        exit(1);
    }

    for(int i = 0; i < worker_num; i++) {
        _workers[i].init(configserver, group_id, area, key_format);
    }
}

TairKeyCleanTool::~TairKeyCleanTool() {
    if (_keyfile != NULL) {
        delete []_keyfile;
    }
    if (_workers != NULL) {
        delete []_workers;
    }
}

void TairKeyCleanTool::execute() {
    for(int i = 0; i < _worker_num; i++) {
        _workers[i].start();
    }
    readFile();
}

void TairKeyCleanTool::readFile() {
    FILE* fp = NULL;
    fp = fopen(_keyfile, "r");
    if (fp == NULL) {
        fprintf(stderr, "file %s, line %d: file open failed\n",
                __FILE__, __LINE__);
        exit(1);
    }

    char buffer[BUFF_SIZE];
    DelJob *job = new DelJob(_key_op_max);
    if (job == NULL) {
        fprintf(stderr, "file %s, line %d: no enough memory\n",
                __FILE__, __LINE__);
        exit(1);
    }
    while(fgets(buffer, GETS_SIZE, fp) != NULL) {
        if (job->getSize() == _key_op_max) {
            postJob(job);
            job = new DelJob(_key_op_max);
            if (job == NULL) {
                fprintf(stderr, "file %s, line %d: no enough memory\n",
                        __FILE__, __LINE__);
                exit(1);
            }
        }
        int size = strlen(buffer);
        buffer[size - 1] = '\0';
        job->addKey(buffer, size);
    }

    if (job != NULL) {
        if (job->getSize() != 0) {
            postJob(job);
        } else {
            delete job;
        }
    }

    if (fp != NULL) {
        fclose(fp);
    }
}

void TairKeyCleanTool::postJob(DelJob* job) {
    int ok = false;
    while(true) {
        ok = _workers[_worker_selected].getJob(job);
        if (ok == true) {
            break;
        }
        _worker_selected = (_worker_selected + 1) % _worker_num; 
    }
}

void TairKeyCleanTool::helper() {
    fprintf(stdout, "keycleantool tool.conf\n");
}
}



/*********************************************Main******************************************/

//configserver
//group_name
//area
//keyfile

int main(int argc, char** argv) {
    if (argc != 2) {
        tair::TairKeyCleanTool::helper();
        return 1;
    }

    ConfigLoader loader;
    loader.loadInfo(argv[1]);
    
    char* configserver = loader.getMsConfigServ();
    if (configserver == NULL) {
        fprintf(stderr, "configserver == NULL\n");
        return 1;
    }

    char* groupname = loader.getGroupName();
    if (groupname == NULL) {
        fprintf(stderr, "groupname == NULL\n");
        return 1;
    }

    int area = loader.getNameSpace();
    char* keyfile = loader.getKeyFile();
    if (keyfile == NULL) {
        fprintf(stderr, "keyfile == NULL\n");
        return 1;
    }

    int worker_num = loader.getWorkerNum();
    int deljobSize = loader.getDelJobSize();
    int key_format = loader.getKeyFormat();

    fprintf(stdout, "configserver %s\n", configserver);
    fprintf(stdout, "groupname %s\n", groupname);
    fprintf(stdout, "area %d\n", area);
    fprintf(stdout, "keys file %s\n", keyfile);
    fprintf(stdout, "worker num %d\n", worker_num);
    fprintf(stdout, "del job size %d\n", deljobSize);
    fprintf(stdout, "key format %d\n", key_format);

    TBSYS_LOGGER.setLogLevel("ERROR");
    tair::TairKeyCleanTool tool(configserver, groupname, area,
            keyfile, key_format, worker_num, deljobSize);
    tool.execute();

    return 0;
}
#endif
