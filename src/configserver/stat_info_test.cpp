/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * stat_info_test.cpp is for testing the class stat_info.
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "stat_info.hpp"
using namespace tair;
using namespace config_server;

int
main()
{
  stat_info_detail s;
  assert(s.get_unit_size() == 0);
  assert(s.get_unit_value(10) == 0);
  assert(s.get_unit_size() == 0);
  assert(s.set_unit_value(10, 255) == 255);

  assert(s.get_unit_size() == 11);
  assert(s.get_unit_value(10) == 255);
  assert(s.get_unit_value(9) == 0);
  assert(s.get_unit_value(11) == 0);
  assert(s.get_unit_size() == 11);

  stat_info_detail t;
  t.insert_stat_info_detail(s);
  assert(t.get_unit_size() == 11);
  assert(t.get_unit_value(10) == 255);
  assert(t.get_unit_value(9) == 0);
  assert(t.get_unit_value(11) == 0);
  assert(t.get_unit_size() == 11);


  t.update_stat_info_detail(s);
  assert(t.get_unit_size() == 11);
  assert(t.get_unit_value(10) == 255 * 2);
  assert(t.get_unit_value(9) == 0);
  assert(t.get_unit_value(11) == 0);
  assert(t.get_unit_size() == 11);


  t.insert_stat_info_detail(s);
  assert(t.get_unit_size() == 11);
  assert(t.get_unit_value(10) == 255);
  assert(t.get_unit_value(9) == 0);
  assert(t.get_unit_value(11) == 0);
  assert(t.get_unit_size() == 11);

  t.clear();
  t.update_stat_info_detail(s);
  assert(t.get_unit_size() == 11);
  assert(t.get_unit_value(10) == 255);
  assert(t.get_unit_value(9) == 0);
  assert(t.get_unit_value(11) == 0);
  assert(t.get_unit_size() == 11);

  assert(s.set_unit_value(11, 256) == 256);
  assert(s.get_unit_size() == 12);
  assert(s.get_unit_value(10) == 255);
  assert(s.get_unit_value(9) == 0);
  assert(s.get_unit_value(11) == 256);
  assert(s.get_unit_size() == 12);

  t.update_stat_info_detail(s);

  assert(t.get_unit_size() == 12);
  assert(t.get_unit_value(10) == 255 * 2);
  assert(t.get_unit_value(9) == 0);
  assert(t.get_unit_value(11) == 256);
  assert(t.get_unit_size() == 12);

  std::map<uint32_t, stat_info_detail> data;

  node_stat_info n;
  n.insert_stat_detail(2, t);
  data = n.get_stat_data();
  assert(data.size() == 1);
  assert(data.begin()->first == 2);

  n.update_stat_info(2, t);
  n.update_stat_info(3, t);

  data = n.get_stat_data();
  assert(data.size() == 2);
  assert(data.begin()->first == 2);
  assert(data[2].get_unit_size() == 12);
  assert(data[2].get_unit_value(10) == 255 * 2 * 2);
  assert(data[2].get_unit_value(9) == 0);
  assert(data[2].get_unit_value(11) == 256 * 2);

  printf("all ok\n");
  return 0;
}
