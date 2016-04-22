#include "tair_statistics.h"
#include <stdio.h>
#include <tbsys.h>
#include <define.hpp>

using namespace std;

namespace tair {

int64_t tair_statistics::get_area_stat(int area, const char *desc) {
    unordered_map<string, uint32_t>::const_iterator item_pos_it = _desc_fast_search.find(desc);
    if (UNLIKELY(_desc_fast_search.end() == item_pos_it))
        return -2;
    char area_str[8];
    snprintf(area_str, sizeof(area_str), "%d", area);
    map<string, vector<int64_t> >::const_iterator area_stat_it = _area_stat.find(area_str);
    if (UNLIKELY(_area_stat.end() == area_stat_it))
        return -1;
    return area_stat_it->second[item_pos_it->second];
}

void tair_statistics::load_data(const vector<string> &desc, const map<uint64_t, vector<int64_t> > &original_stat) {
    if (desc.empty())
        return;
    clear();
    const size_t stat_num = desc.size() - 1;
    map<string, vector<int64_t> > tmp_stat;
    for (map<uint64_t, vector<int64_t> >::const_iterator it = original_stat.begin(); it != original_stat.end(); ++it) {
        // one ds stat
        const vector<int64_t> &one_ds_stat = it->second;
        string ds(tbsys::CNetUtil::addrToString(it->first));
        if (one_ds_stat.size() % (stat_num + 1) != 0)
            continue;
        for (size_t row = 0; row < one_ds_stat.size(); row += stat_num + 1) {
            char area[16];
            snprintf(area, 16, "%ld", one_ds_stat[row]);
            string ds_area = ds + ":" + area;
            vector<int64_t> &one_ds_area_stat = tmp_stat[ds_area];
            one_ds_area_stat.assign(one_ds_stat.begin() + row + 1, one_ds_stat.begin() + row + 1 + stat_num);
        }
    }
    _server_area_stat.swap(tmp_stat);
    _desc.assign(desc.begin() + 1, desc.end());
    for (size_t i = 0; i < _desc.size(); ++i)
        _desc_fast_search.insert(unordered_map<string, uint32_t>::value_type(_desc[i], (uint32_t) i));
    // calculate area stat to _area_stat,
    // _server_stat is calculated on demond.
    cal_other_stat(area);
}

void tair_statistics::clear() {
    _desc.clear();
    _desc_fast_search.clear();
    _area_stat.clear();
    _server_stat.clear();
    _server_area_stat.clear();
}

void tair_statistics::cal_other_stat(int dest) {
    for (map<string, vector<int64_t> >::const_iterator it = _server_area_stat.begin();
         it != _server_area_stat.end(); ++it) {
        vector<int64_t> *one_stat;
        size_t pos = it->first.find_last_of(':');
        if (string::npos == pos)
            continue;

        if (area == dest) {
            string area(it->first.substr(pos + 1));
            one_stat = &(_area_stat[area]);
        } else {
            string server(it->first.substr(0, pos));
            one_stat = &(_server_stat[server]);
        }
        if (one_stat->empty())
            one_stat->assign(it->second.size(), 0);
        for (size_t i = 0; i < it->second.size(); ++i)
            (*one_stat)[i] += it->second[i];
    }
}

}

