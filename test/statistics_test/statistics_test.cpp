#include "tair_client_api.hpp"
#include "inval_stat.hpp"
#include <string>
#include <map>
#include <iomanip>

using namespace std;
using namespace tair;

const int INVALID = 0;
const int PREFIX_INVALID = 1;
const int HIDE = 2;
const int PREFIX_HIDE =3;
const int MAX_TIMES = 10;
const int BASE_TIMES = 50;
string config;
string group_name;
struct test_data_t;
void print_invalid_server_stat(uint64_t sid, inval_stat_data_t &stat, map<string, map<int, test_data_t*> > &md);
//define the parameter of thread
struct test_data_t {
  int invalid;
  int prefix_invalid;
  int hide;
  int prefix_hide;

  int invalid_count[2];
  int prefix_invalid_count[2];
  int hide_count[2];
  int prefix_hide_count[2];

  string config;
  string group_name;
  int area;

  tair_client_api* client;
  test_data_t():invalid(0),prefix_invalid(0),hide(0),prefix_hide(0){
    invalid_count[0] = 0;
    invalid_count[1] = 0;
    prefix_invalid_count[0] = 0;
    prefix_invalid_count[1] = 0;
    hide_count[0] = 0;
    hide_count[1] = 0;
    prefix_hide_count[0] = 0;
    prefix_hide_count[1] = 0;
    client = 0;
  }
  ~test_data_t(){
    if(client != NULL) {
      delete client;
    }
  }
};

data_entry* generate_data_entry() {
  static char* cp ="qwertyuiop[]asddfghjkl;lzxcvbnm,./'][;.,lpokmijnuhbygvtfcrdxeszwaq";

  int size = strlen(cp);
  int start = 0;
  int end = 0;
  while(start >= end) {
    start = rand() % size;
    end = rand() % size;
  }
  char *p =cp+start;
  int length = end - start + 1;
  char *d = new char [length+1];
  strncpy(d,p,length);
  d[length] = '\0';

  return new data_entry(d, length, false);
}
//test
void* business_run(void *args) {
  int ret = -1;
  test_data_t *stat = (test_data_t*)args;
  srand(tbsys::CTimeUtil::getTime());
  stat->invalid = rand() % MAX_TIMES + BASE_TIMES;
  stat->prefix_invalid = rand() % MAX_TIMES + BASE_TIMES;
  stat->hide = rand() % MAX_TIMES + BASE_TIMES;
  stat->prefix_hide= rand() % MAX_TIMES + BASE_TIMES;

  if(stat == NULL) {
    cout<<"error!"<<endl;
  }
  //connect the server
  stat->client = new tair_client_api();
  if(!stat->client->startup(stat->config.c_str(), NULL, stat->group_name.c_str())) {
    cout <<"config:"<<stat->config <<" group_name:"<<stat->group_name<<endl;
    cout<<"start up failed"<<endl;
    exit(0);
  }
  stat->client->set_timeout(500);
  //hide
  int hide_counter = 0;
  int invalid_counter = 0;
  int prefix_hide_counter = 0;
  int prefix_invalid_counter = 0;
  int counter = 0;
  int size = stat->hide + stat->prefix_hide + stat->invalid +stat->prefix_invalid;

  while(counter < size)
  {
    while(hide_counter < stat->hide)
    {
      data_entry *key = generate_data_entry();
      ret = stat->client->hide_by_proxy(stat->area, *key);
      if(ret == TAIR_RETURN_SUCCESS) stat->hide_count[0]++;
      else stat->hide_count[1]++;
      delete key;
      hide_counter++;
      counter++;
      if(rand()%10 == 0) break;
    }
    while(invalid_counter < stat->invalid)
    {
      data_entry *key = generate_data_entry();
      ret = stat->client->invalidate(stat->area, *key);
      if(ret == TAIR_RETURN_SUCCESS) stat->invalid_count[0]++;
      else stat->invalid_count[1]++;
      delete key;
      prefix_invalid_counter++;
      counter++;
      if(rand()%10 == 0) break;
    }
    while(prefix_invalid_counter < stat->prefix_invalid)
    {
      data_entry *key = generate_data_entry();
      ret = stat->client->prefix_invalidate(stat->area, *key);
      if(ret == TAIR_RETURN_SUCCESS) stat->prefix_invalid_count[0]++;
      else stat->prefix_invalid_count[1]++;
      delete key;
      prefix_invalid_counter++;
      counter++;
      if(rand()%10 == 0) break;
    }

    while(prefix_hide_counter < stat->prefix_hide)
    {
      data_entry *key = generate_data_entry();
      ret = stat->client->prefix_hide_by_proxy(stat->area, *key);
      if(ret == TAIR_RETURN_SUCCESS) stat->prefix_hide_count[0]++;
      else stat->prefix_hide_count[1]++;
      delete key;
      prefix_hide_counter++;
      counter++;
      if(rand()%10 == 0) break;
    }
  }
  return 0;
}

void test_invalid_server_stat() {
  int client_size = 120;
  int first = client_size * 0.3;
  int second = client_size * 0.6;
  test_data_t * test_data = new test_data_t[client_size];
  pthread_t *pid = new pthread_t[client_size];
  for (int i = 0; i < client_size; i++) {
    test_data[i].config = config;
    test_data[i].group_name = group_name;
    if (i < first) {
      test_data[i].area = 0;
    }
    else if (i >= first && i < second) {
      test_data[i].area = 1;
    }
    else {
      test_data[i].area = 2;
    }
  }
  bool again = true;
  do{
    //create thread;
    for (int i = 0; i < client_size; ++i) {
      if (pthread_create(&pid[i], NULL, business_run, (void*)(&(test_data[i])))) {
        cout<<"failed to create thread. i="<<i<<endl;
      }
    }
    //wait
    for (int i = 0; i < client_size; ++i) {
      pthread_join(pid[i], NULL);
    }
    //here, get the stats information.
    unsigned long data_size = 0;
    int group_count = 0;
    tair_client_api client;
    if(!client.startup(config.c_str(), NULL, group_name.c_str())) {
      cout<<"error, failed to connect the server."<<endl;
    }
    //summary
    map<string, map<int, test_data_t*> > done_data_map;
    for (int i = 0; i < client_size; ++i) {
      if (done_data_map.find(test_data[i].group_name) == done_data_map.end()) {
        map<int, test_data_t*> mt;
        mt[0]=new test_data_t();
        mt[0]->area = 0;
        mt[0]->group_name = test_data[i].group_name;
        mt[1]=new test_data_t();
        mt[1]->area = 1;
        mt[1]->group_name = test_data[i].group_name;
        mt[2]=new test_data_t();
        mt[2]->area = 2;
        mt[2]->group_name = test_data[i].group_name;
        done_data_map[test_data[i].group_name] = mt;
      }
      map<int, test_data_t*> &done_data_mt = done_data_map[test_data[i].group_name];
      test_data_t & done_data = *(done_data_mt[test_data[i].area]);
      done_data.invalid += test_data[i].invalid;
      done_data.prefix_invalid += test_data[i].prefix_invalid;
      done_data.hide += test_data[i].hide;
      done_data.prefix_hide += test_data[i].prefix_hide;

      done_data.invalid_count[0] += test_data[i].invalid_count[0];
      done_data.invalid_count[1] += test_data[i].invalid_count[1];
      done_data.prefix_invalid_count[0] += test_data[i].prefix_invalid_count[0];
      done_data.prefix_invalid_count[1] += test_data[i].prefix_invalid_count[1];
      done_data.hide_count[0] += test_data[i].hide_count[0];
      done_data.hide_count[1] += test_data[i].hide_count[1];
      done_data.prefix_hide_count[0] += test_data[i].prefix_hide_count[0];
      done_data.prefix_hide_count[1] += test_data[i].prefix_hide_count[1];

    }
    //show stats information
    do {
      int ret = -1;
      map<uint64_t, inval_stat_data_t*> stats;
      ret = client.query_from_invalidserver(stats);
      if (ret != TAIR_RETURN_SUCCESS) {
        cout<<"error, failed to get stats data."<<endl;
        return;
      }
      for (map<uint64_t, inval_stat_data_t*>::iterator it = stats.begin();
          it != stats.end();
          it++) {
        if (it->second != 0) {
          uint64_t sid = it->first;
          inval_stat_data_t* stat = it->second;
          int group_count = stat->group_count;
          print_invalid_server_stat(sid, *stat, done_data_map);
          delete it->second;
        }
      }
      do {
        cout << "(1- show again; 2- new testing;  0- exit";
        cin >> ret;
        if (!(ret == 1 || ret == 0 || ret == 2)) {
          cout<<"error, input 1 , 2 or 0 "<<endl;
        }
        else break;
      }while(true);

      if(ret == 0){
        again = false;
        break;
      }
      if(ret == 2) {
        again = true;
        break;
      }
    }while(true);
    for (map<string, map<int, test_data_t*> >::iterator it =done_data_map.begin();
        it != done_data_map.end(); it++) {
      map<int,test_data_t*> & tmp =it->second;
      for (map<int, test_data_t*>::iterator i = tmp.begin(); i != tmp.end(); i++) {
        if (i->second)
          delete i->second;
      }
    }
  }while(again);
  delete [] test_data;
  delete [] pid;
}

  int get_stat_done(int type,int op_type, inval_area_stat& as){
    switch(type)
    {
      case INVALID:
        return as.get_invalid_count(op_type);
      case PREFIX_INVALID:
        return as.get_prefix_invalid_count(op_type);
      case HIDE:
        return as.get_hide_count(op_type);
      case PREFIX_HIDE:
        return as.get_prefix_hide_count(op_type);
      default:
        return 0;
    }
  }
inline void __print_tabs(int w) {
  int i=0;
  while (i++ < w) cout << " ";
}
inline void __print_line(int l) {
  int i = 0;
  while (i++ < l) cout << "_";
  cout << endl;
}
void print_area_stat(inval_area_stat& as, test_data_t* td)
{
  vector<string> ops;
  ops.push_back("invalid");
  ops.push_back("prefix_invalid");
  ops.push_back("hide");
  ops.push_back("prefix_hide");
  int push = 0;
  int width = 5;
  for (int i = 0; i < ops.size(); ++i) {
    string op = ops [i];
    int type = -1;
    int wsize  = 4;
    if(op == "invalid") {
      type = INVALID;
      wsize += 7;
      push = td->invalid;
    }
    else if(op == "prefix_invalid") {
      type = PREFIX_INVALID;
      push = td->prefix_invalid;
    }
    else if(op == "hide") {
      type = HIDE;
      wsize += 10;
      push = td->hide;
    }
    else if(op == "prefix_hide") {
      type = PREFIX_HIDE;
      wsize += 3;
      push = td->prefix_hide;
    }
    cout<<"OPERATION: "<<op;
    __print_tabs(wsize);
    int first_do = 0;
    int retry_do = 0;
    int finlly_do = 0;
    first_do = get_stat_done(type, inval_area_stat::FIRST_EXEC, as);
    retry_do = get_stat_done(type, inval_area_stat::RETRY_EXEC, as);
    finlly_do = get_stat_done(type,  inval_area_stat::FINALLY_EXEC, as);
    cout << setw(width) << first_do;
    cout << "/" << setw(width) << retry_do << "/" << setw(width) << finlly_do;
    cout << "/" << setw(width) << push;
    __print_tabs(10);
    cout << " [" << td->area << "]   ";
    cout << endl;
  }
}
void print_group_stat(inval_group_stat& group_stat, map<int, test_data_t*> &mt)
{
  cout << "group name: " << group_stat.get_group_name() << endl;
  for (map<int, test_data_t*>::iterator it = mt.begin(); it != mt.end(); it++) {
    if (it->second != NULL) {
      inval_area_stat& as = group_stat.get_area_stat(it->second->area);
      print_area_stat(as,it->second);
    }
    else {
      cout << "error , print_group_stat" << endl;
    }
  }
}
void print_invalid_server_stat(uint64_t server_id, inval_stat_data_t &stat, map<string, map<int, test_data_t*> > &md)
{
  __print_line(80);
  string inval_server = tbsys::CNetUtil::addrToString(server_id);
  cout << "invalid server: " << inval_server << endl;
  inval_group_stat* group_stat = (inval_group_stat*)stat.stat_data;
  for (int i = 0; i< stat.group_count; ++i){
    inval_group_stat* gs = group_stat + i;
    string gname(gs->get_group_name());
    map<string, map<int, test_data_t*> >::iterator it = md.find(gname);
    if (it == md.end()) {
      cout << "can't find the group :" << gname << endl;
      continue;
    }
    print_group_stat(*gs, it->second);
  };

}
void test_invalid_server_start_single(){

  tair_client_api client;
  if (!client.startup(config.c_str(), NULL, group_name.c_str())) {
    cout << "error, failed to connect the server." << endl;
    return ;
  }
  do {
    int ret = -1;
    for (int i = 0; i < 50; i++) {
      data_entry *key = generate_data_entry();
      ret = client.prefix_invalidate(1, *key, group_name.c_str());
      if (ret != TAIR_RETURN_SUCCESS) {
        cout << "failed to invalidate! i= " << i << endl;
      }
      delete key;
    }
    while (true) {
      map<uint64_t, inval_stat_data_t*> stats;
      ret = client.query_from_invalidserver(stats);
      if (ret != TAIR_RETURN_SUCCESS) {
        cout<<"error, failed to get stats data."<<endl;
        return;
      }
      for(map<uint64_t, inval_stat_data_t*>::iterator it = stats.begin();
          it != stats.end();
          it++) {
        inval_stat_data_t* t = it->second;
        if (t == NULL) continue;
        inval_group_stat* _gs = (inval_group_stat*)t->stat_data;
        for (int i=0;i<t->group_count;i++){
          inval_group_stat* gs=_gs+i;
          int value = gs->get_area_stat(0).get_prefix_invalid_count(0);
          cout << "area = 1 ,invalid = " << value << endl;
        }
        delete t;
      }
      cout << "input:1- continue, 0- exit";
      cin >> ret;
      if (ret == 0) break;
    };
    cout << "input :(1-continue, 0- exit)";
    cin >> ret;
    if (ret == 0 ) break;
  }while(true);
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("ERROR");
  // tair_client_api client_handle;
  cout << "==============================TEST INVALID SERVER=========================" << endl;
  string ip;
  string port;
  cout << "input configserver ip:";
  cin >> ip;
  cout <<" input configserver port:";
  cin >> port;
  cout <<" input group's name:";
  cin >> group_name;
  config = ip + ":" + port;
  cout <<config<<endl;
  // test_invalid_server_start_single();
  test_invalid_server_stat();
  //connect the server
  tair_client_api* client = new tair_client_api();
  if (client->startup(config.c_str(), NULL, group_name.c_str())) {
    cout<<"start up failed"<<endl;
    exit(0);
  }
  client->set_timeout(1000);
  //invalidate
  int ret = -1;
  while(true)
  {
    int in;
    cout << "input:(1- continue, 0- exit)";
    cin >> in;
    if (in == 0) break;
    data_entry *key = generate_data_entry();
    ret = client->invalidate(1, *key, group_name.c_str());
    if (ret == TAIR_RETURN_SUCCESS) cout<<"ok"<<endl;
    else cout<<"failed"<<endl;
    delete key;
  }
  delete client;
  // test_invalid_server_start_single();
  return 0;
}

