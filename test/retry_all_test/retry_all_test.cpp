#include "tair_client_proxy.hpp"
#include "inval_stat.hpp"
#include <string>
#include <map>
#include <iomanip>
#include <vector>

using namespace std;

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  cout<<"==============================TEST INVALID SERVER========================="<<endl;
  string ip;
  string port;
  string group_name;
  cout << "input configserver ip:";
  cin >> ip;
  cout << "input configserver port:";
  cin >> port;
  cout << "input group's name:";
  cin >> group_name;
  string config= ip + ":" + port;
  vector<std::string> group_list;
  tair_client_proxy client;
  client.get_group_name_list(tbsys::CNetUtil::strToAddr(ip.c_str(),50565), 0, group_list);
  cout << "size :" << group_list.size() << endl;
  for (int i = 0; i < group_list.size(); ++i) {
    cout << "group name: " << group_list[i] << endl;
  }
  tair_client_proxy* pclient = new tair_client_proxy();
  if (!pclient->startup(config.c_str(), NULL, group_name.c_str())) {
    cout << "start up failed" << endl;
    exit(0);
  }
  pclient->set_timeout(1000);
  //invalidate
  int ret = -1;
  while (true)
  {
    vector<string> infos;
    vector<uint64_t> ivs;
    pclient->retrieve_invalidserver(ivs);
    for (int i = 0; i < ivs.size(); ++i) {
      uint64_t id = ivs[i];
      cout << " --->(1- continue, 2- edit, 0- retry all) input:";
      cin >> ret;
      if (ret == 2) {
        infos.clear();
        int add_request_storage = 0;
        int area = -1;
        string group_name;
        cout << "input the group name:";
        cin >> group_name;
        cout << "input the area:";
        cin >> area;
        cout << "input the add_request_storage:";
        cin >> add_request_storage;
        char buffer[256];
        memset(buffer, '\0', 256);
        sprintf(buffer, "group_name=%s", group_name.c_str());
        infos.push_back(buffer);
        memset(buffer, '\0', 256);
        sprintf(buffer, "area=%d", area);
        infos.push_back(buffer);
        memset(buffer, '\0', 256);
        sprintf(buffer, "add_request_storage=%8d", add_request_storage);
        infos.push_back(buffer);
      }
      else if(ret == 0) {
        ret = pclient->retry_all();
        if (ret == TAIR_RETURN_SUCCESS)
          cout << "retry all, done!" << endl;
        else
          cout << "failed to retry all";
        continue;
      }
      ret = pclient->debug_support(id, infos);
      for (int i = 0; i < infos.size(); ++i) {
        cout << "---->" << infos[i] << endl;
      }
    }
    if (ret == TAIR_RETURN_SUCCESS) cout << "ok" << endl;
    else cout << "failed" << endl;
    int in;
    cout << "input:(1- continue, 0- exit)";
    cin >> in;
    if (in == 0) break;
  }
  delete pclient;
  return 0;
}
