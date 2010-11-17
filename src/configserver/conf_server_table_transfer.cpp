/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * conf_server_table_transfer..cpp is a tool to modify the hashtable config server builded
 *
 * Version: $Id$
 *
 * Authors:
 *      Daoan <daoan@taobao.com>
 *
 */
#include "conf_server_table_manager.hpp"
using namespace tair;
using namespace
  tair::config_server;
int
main(int argc, char **argv)
{
  if(argc != 3) {
    printf("transfer txtfile binaryfile\n");
    return 0;
  }
  conf_server_table_manager
    cstm;
  if(cstm.translate_from_txt2binary(argv[1], argv[2]) == false) {
    printf("transfer error\n");
    return 0;
  }
  printf("transfer ok\n");
  return 0;

}
