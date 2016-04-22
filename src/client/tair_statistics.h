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

#ifndef _TAIR_STATISTICS_IMPL_H_
#define _TAIR_STATISTICS_IMPL_H_

#include <vector>
#include <string>
#include <map>
#include <stdint.h>

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std
{
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

namespace tair {

class tair_statistics {
protected:
    enum {
        server,
        area
    };

public:
    friend class tair_client_impl;

    const size_t get_stat_item_size() const { return _desc.size(); }

    /*
      @return store the stat description, e.g. <"put", "put_key", "get", "get_key", "hit", ... >

      mdb:
      <"put", "put_key", "get", "get_key", "hit", "remove_key", "hide", "add", "pf_put", "pf_put_key", "pf_get",
      "pf_get_key", "pf_hit", "pf_remove_key", "pf_hide", "get_range", "get_range_key", "fc_in", "fc_out",
      "migrate_key", "migrate_flow_in", "evict", "itemcount", "datasize", "usesize", "quota">

      ldb:
      <"put", "put_key", "get", "get_key", "hit", "remove_key", "hide", "add", "pf_put", "pf_put_key", "pf_get",
      "pf_get_key", "pf_hit", "pf_remove_key", "pf_hide", "get_range", "get_range_key", "fc_in", "fc_out",
      "migrate_key", "migrate_flow_in", "bf_get", "bf_hit", "ca_get", "ca_hit", "evict", "user_itemcount", "user_datasize",
      "physical_itemcount", "physical_datasize", "itemcount", "datasize", "usesize", "quota">

      ldb without bloomfilter:
      <"put", "put_key", "get", "get_key", "hit", "remove_key", "hide", "add", "pf_put", "pf_put_key", "pf_get",
      "pf_get_key", "pf_hit", "pf_remove_key", "pf_hide", "get_range", "get_range_key", "fc_in", "fc_out",
      "migrate_key", "migrate_flow_in", "ca_get", "ca_hit", "evict", "user_itemcount", "user_datasize",
      "physical_itemcount", "physical_datasize", "itemcount", "datasize", "usesize", "quota">
      no "bf_get" and "bf_hit".
    */
    const std::vector<std::string> &get_stat_desc() const { return _desc; }

    /*
      @return   the server:area stat data,
      e.g.
      "xxx.xxx.xxx.xxx:15002:0"     -> [a0, b0, c0, d0, e0, f0, ...]
      "xxx.xxx.xxx.xxx:15002:65535" -> [a1, b1, c1, d1, e1, f1, ...]
      "xxx.xxx.xxx.xxx:15002:0"     -> [a2, b2, c2, d2, e2, f2, ...]
      "xxx.xxx.xxx.xxx:15002:65535" -> [a3, b3, c3, d3, e3, f3, ...]

      xxx.xxx.xxx.xxx:15002:65535, xxx.xxx.xxx.xxx:15002, means server ip:port, 65535 means area.
      a[0-3] correspond to _desc[0] stat number, b[0-3] correspond to  _desc[1] stat number, and so on.
      And make sure that:
      foreach (map<string, vector<int64_t> >::iterator it: _server_area_stat)
        get_stat_item_size() == it->second.size()
    */
    const std::map<std::string, std::vector<int64_t> > &get_server_area_stat() const { return _server_area_stat; }

    /*
      @return  the area stat data,
      e.g.
      "0"     -> [h0, i0, j0, k0, l0, m0, ...]
      "65535" -> [h1, i1, j1, k1, l1, m1, ...]

      0, 65535 means area.
      h[0-3] correspond to _desc[0] stat number, i[0-3] correspond to  _desc[1] stat number, and so on.
      And make sure that:
      foreach (map<string, vector<int64_t> >::iterator it: _area_stat)
        get_stat_item_size() == it->second.size()
    */
    const std::map<std::string, std::vector<int64_t> > &get_area_stat() const {
        return _area_stat;
    }

    /*
      @return  the area stat data,
      e.g.
      "xxx.xxx.xxx.xxx:15002" -> [p0, q0, r0, s0, t0, u0, ...]
      "xxx.xxx.xxx.xxx:15002" -> [p1, q1, r1, s1, t1, u1, ...]

      xxx.xxx.xxx.xxx:15002, xxx.xxx.xxx.xxx:15002 means server ip:port.
      p[0-1] correspond to _desc[0] stat number, q[0-1] correspond to  _desc[1] stat number, and so on.
      And make sure that:
      foreach (map<string, vector<int64_t> >::iterator it: _server_stat)
        get_stat_item_size() == it->second.size()
    */
    const std::map<std::string, std::vector<int64_t> > &get_server_stat() {
        if (_server_stat.empty())
            cal_other_stat(server);
        return _server_stat;
    }

    /*
      @param area,  specify the area
      @param desc,  specify the item desc

      @return,  the item stat of the area
                -1, area not exist
                -2, desc not exist
     */
    int64_t get_area_stat(int area, const char *desc);

private:
    void
    load_data(const std::vector<std::string> &desc, const std::map<uint64_t, std::vector<int64_t> > &original_stat);

    void clear();

    void cal_other_stat(int dest);

private:
    std::vector<std::string> _desc;
    std::unordered_map<std::string, uint32_t> _desc_fast_search;

    std::map<std::string, std::vector<int64_t> > _area_stat;
    std::map<std::string, std::vector<int64_t> > _server_stat;
    std::map<std::string, std::vector<int64_t> > _server_area_stat;
};

}

#endif
