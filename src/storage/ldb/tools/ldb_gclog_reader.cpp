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

#include <iostream>
#include <fstream>
#include <string>

#include "ldb_gc_factory.hpp"

using namespace std;
using namespace tair::storage::ldb;

void printCont(GcLogRecord glr) {
    time_t time = glr.node_.when_;
    struct tm *local = localtime(&time);
    char buf[20];
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", local->tm_year + 1900, local->tm_mon + 1, local->tm_mday,
            local->tm_hour, local->tm_min, local->tm_sec);
    char rslt[200];

    const char *ops = glr.oper_type_ == 1 ? "add" : "rem";
    const char *type = glr.node_type_ == 1 ? "bucket" : "##area";
    sprintf(rslt, "oper(%s), type(%s), key(%04d), sequece(%016"
    PRI64_PREFIX
    "u), filenum(%010"
    PRI64_PREFIX
    "u), time(%s)",
            ops, type, glr.node_.key_,
            glr.node_.sequence_, glr.node_.file_number_, buf);
    cout << rslt << endl;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "usage : ./ldb_gclog_reader filename" << endl;
        return -1;
    }
    char *filename = argv[1];
    int32_t offset = 64, size = sizeof(GcLogRecord);
    ifstream fin(filename, ios::in);
    if (fin.is_open()) {
        // check the size
        fin.seekg(0, fin.end);
        uint32_t len = fin.tellg();
        if ((len - offset) % size != 0) {
            cout << "size err : " << len << endl;
        }
        fin.seekg(offset, ios::beg);
        uint32_t num = 0;
        char buf[size];

        GcLogRecord glr;
        while (fin.read(buf, size)) {
            glr.decode(buf);
            printCont(glr);
            num++;
        }
        fin.close();
        cout << "total gc record num is " << num << endl;
    }
    return 0;
}
