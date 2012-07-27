/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include "tair_client.hpp"
#include "define.hpp"
#include "item_manager.hpp"
#include "util.hpp"
#include "dump_data_info.hpp"
#include "query_info_packet.hpp"

namespace tair {

   using namespace json;

   tair_client::tair_client()
   {
      is_config_server = false;
      is_data_server = false;
      //server_id = 0;
      //m_slaveServerId = 0;
      group_name = NULL;
      cmd_line = NULL;
      cmd_file_name = NULL;
      is_cancel = false;
      server_addr = NULL;
      slave_server_addr = NULL;

      key_format = 0;
      default_area = 0;

      cmd_map["help"] = &tair_client::do_cmd_help;
      cmd_map["quit"] = &tair_client::do_cmd_quit;
      cmd_map["exit"] = &tair_client::do_cmd_quit;
      cmd_map["put"] = &tair_client::do_cmd_put;
      cmd_map["incr"] = &tair_client::do_cmd_addcount;
      cmd_map["get"] = &tair_client::do_cmd_get;
      cmd_map["mget"] = &tair_client::do_cmd_mget;
      cmd_map["remove"] = &tair_client::do_cmd_remove;
      cmd_map["mremove"] = &tair_client::do_cmd_mremove;
      cmd_map["delall"] = &tair_client::do_cmd_remove_area;
      cmd_map["dump"] = &tair_client::do_cmd_dump_area;
      cmd_map["stat"] = &tair_client::do_cmd_stat;
      cmd_map["hide"] = &tair_client::do_cmd_hide;
      cmd_map["gethidden"] = &tair_client::do_cmd_get_hidden;
      cmd_map["pput"] = &tair_client::do_cmd_prefix_put;
      cmd_map["pget"] = &tair_client::do_cmd_prefix_get;
      cmd_map["premove"] = &tair_client::do_cmd_prefix_remove;
      cmd_map["premoves"] = &tair_client::do_cmd_prefix_removes;
      cmd_map["pgethidden"] = &tair_client::do_cmd_prefix_get_hidden;
      cmd_map["phide"] = &tair_client::do_cmd_prefix_hide;
      cmd_map["phides"] = &tair_client::do_cmd_prefix_hides;
      cmd_map["flowlimit"] = &tair_client::do_cmd_set_flow_limit_bound;
      cmd_map["flowrate"] = &tair_client::do_cmd_get_flow_rate;

      cmd_map["gettmpdownsvr"] = &tair_client::do_cmd_gettmpdownsvr;
      cmd_map["resetserver"] = &tair_client::do_cmd_resetserver;
      cmd_map["set_migrate_wait"] = &tair_client::do_cmd_set_migrate_wait_ms;
      cmd_map["stat_db"] = &tair_client::do_cmd_stat_db;
      cmd_map["release_mem"] = &tair_client::do_cmd_release_mem;
      cmd_map["pause_gc"] = &tair_client::do_cmd_pause_gc;
      cmd_map["resume_gc"] = &tair_client::do_cmd_resume_gc;
      cmd_map["set_config"] = &tair_client::do_cmd_set_config;
      // cmd_map["additems"] = &tair_client::doCmdAddItems;
   }

   tair_client::~tair_client()
   {
      if (group_name) {
         free(group_name);
         group_name = NULL;
      }
      if (cmd_line) {
         free(cmd_line);
         cmd_line = NULL;
      }
      if (cmd_file_name) {
         free(cmd_file_name);
         cmd_file_name = NULL;
      }
    }

   void tair_client::print_usage(char *prog_name)
   {
      fprintf(stderr, "%s -s server:port\n OR\n%s -c configserver:port -g groupname\n\n"
              "    -s, --server           data server,default port:%d\n"
              "    -c, --configserver     default port: %d\n"
              "    -g, --groupname        group name\n"
              "    -l, --cmd_line         exec cmd\n"
              "    -q, --cmd_file         script file\n"
              "    -h, --help             print this message\n"
              "    -v, --verbose          print debug info\n"
              "    -V, --version          print version\n\n",
              prog_name, prog_name, TAIR_SERVER_DEFAULT_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
   }

   bool tair_client::parse_cmd_line(int argc, char *const argv[])
   {
      int opt;
      const char *optstring = "s:c:g:hVvl:q:";
      struct option longopts[] = {
         {"server", 1, NULL, 's'},
         {"configserver", 1, NULL, 'c'},
         {"groupname", 1, NULL, 'g'},
         {"cmd_line", 1, NULL, 'l'},
         {"cmd_file", 1, NULL, 'q'},
         {"help", 0, NULL, 'h'},
         {"verbose", 0, NULL, 'v'},
         {"version", 0, NULL, 'V'},
         {0, 0, 0, 0}
      };

      opterr = 0;
      while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) {
         switch (opt) {
            case 'c': {
               if (server_addr != NULL && is_config_server == false) {
                  return false;
               }
               //uint64_t id = tbsys::CNetUtil::strToAddr(optarg, TAIR_CONFIG_SERVER_DEFAULT_PORT);

               if (server_addr == NULL) {
                  server_addr = optarg;
               } else {
                  slave_server_addr = optarg;
               }
               is_config_server = true;
            }
               break;
            case 's': {
               if (server_addr != NULL && is_config_server == true) {
                  return false;
               }
               server_addr = optarg;
               is_data_server = true;
               //server_id = tbsys::CNetUtil::strToAddr(optarg, TAIR_SERVER_DEFAULT_PORT);
            }
               break;
            case 'g':
               group_name = strdup(optarg);
               break;
            case 'l':
               cmd_line = strdup(optarg);
               break;
            case 'q':
               cmd_file_name = strdup(optarg);
               break;
            case 'v':
               TBSYS_LOGGER.setLogLevel("DEBUG");
               break;
            case 'V':
               fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
               return false;
            case 'h':
               print_usage(argv[0]);
               return false;
         }
      }
      if (server_addr == NULL || (is_config_server == true && group_name == NULL)) {
         print_usage(argv[0]);
         return false;
      }
      return true;
   }

   cmd_call tair_client::parse_cmd(char *key, VSTRING &param)
   {
      cmd_call  cmdCall = NULL;
      char *token;
      while (*key == ' ') key++;
      token = key + strlen(key);
      while (*(token-1) == ' ' || *(token-1) == '\n' || *(token-1) == '\r') token --;
      *token = '\0';
      if (key[0] == '\0') {
         return NULL;
      }
      token = strchr(key, ' ');
      if (token != NULL) {
         *token = '\0';
      }
      str_cmdcall_map_iter it = cmd_map.find(tbsys::CStringUtil::strToLower(key));
      if (it == cmd_map.end()) {
         return NULL;
      } else {
         cmdCall = it->second;
      }
      if (token != NULL) {
         token ++;
         key = token;
      } else {
         key = NULL;
      }
      param.clear();
      while ((token = strsep(&key, " ")) != NULL) {
         if (token[0] == '\0') continue;
         param.push_back(token);
      }
      return cmdCall;
   }

   void tair_client::cancel()
   {
      is_cancel = true;
   }

   bool tair_client::start()
   {
      bool done = true;
      client_helper.set_timeout(5000);
      if (is_config_server) {
         done = client_helper.startup(server_addr, slave_server_addr, group_name);
      } else {
        if(is_data_server) {
          done = client_helper.directup(server_addr);
        } else {
          //done = client_helper.startup(server_id);
          done = false;
        }
      }
      if (done == false) {
         fprintf(stderr, "%s cann't connect.\n", server_addr);
         return false;
      }

      char buffer[CMD_MAX_LEN];
      VSTRING param;

      if (cmd_line != NULL) {
         strcpy(buffer, cmd_line);
         vector<char*> cmd_list;
         tbsys::CStringUtil::split(buffer, ";", cmd_list);
         for(uint32_t i=0; i<cmd_list.size(); i++) {
            cmd_call this_cmd_call = parse_cmd(cmd_list[i], param);
            if (this_cmd_call == NULL) {
               continue;
            }
            (this->*this_cmd_call)(param);
         }
      } else if (cmd_file_name != NULL) {
         FILE *fp = fopen(cmd_file_name, "rb");
         if (fp != NULL) {
            while(fgets(buffer, CMD_MAX_LEN, fp)) {
               cmd_call this_cmd_call = parse_cmd(buffer, param);
               if (this_cmd_call == NULL) {
                  fprintf(stderr, "unknown command.\n\n");
                  continue;
               }
               (this->*this_cmd_call)(param);
            }
            fclose(fp);
         } else {
            fprintf(stderr, "open failure: %s\n\n", cmd_file_name);
         }
      } else {
         while (done) {
#ifdef HAVE_LIBREADLINE
           char *line = input(buffer, CMD_MAX_LEN);
           if (line == NULL) break;
#else
            if (fprintf(stderr, "TAIR> ") < 0) break;
            if (fgets(buffer, CMD_MAX_LEN, stdin) == NULL) {
              break;
            }
#endif
            cmd_call this_cmd_call = parse_cmd(buffer, param);
            if (this_cmd_call == NULL) {
               fprintf(stderr, "unknown command.\n\n");
               continue;
            }
            if (this_cmd_call == &tair_client::do_cmd_quit) {
               break;
            }
            (this->*this_cmd_call)(param);
            is_cancel = false;
         }
      }

      // stop
      client_helper.close();

      return true;
   }

   //! you should duplicate the result
#ifdef HAVE_LIBREADLINE
   char* tair_client::input(char *buffer, size_t size) {
     static const char *prompt = "[0;31;40mTAIR>[0;37;40m ";
     char *line = readline(prompt);
     if (line == NULL) return NULL; //~ EOF reveived
     if (*line == '\0') {
       free(line);
       HIST_ENTRY *hist = previous_history();
       if (hist != NULL) {
         strncpy(buffer, hist->line, size);
         return buffer;
       }
       while ((line = readline(prompt)) != NULL && *line == '\0') free(line);
       if (line == NULL) return NULL;
     }
     update_history(line);
     strncpy(buffer, line, size);
     free(line);
     return buffer;
   }
   void tair_client::update_history(const char *line) {
     int i = 0;
     HIST_ENTRY **the_history = history_list();
     for (; i < history_length; ++i) {
       HIST_ENTRY *hist = the_history[i];
       if (strcmp(hist->line, line) == 0) {
         break;
       }
     }
     if (i == history_length) {
       add_history(line);
       return ;
     }
     HIST_ENTRY *hist = the_history[i];
     for (; i < history_length - 1; ++i) {
       the_history[i] = the_history[i+1];
     }
     the_history[i] = hist;
   }
#endif

///////////////////////////////////////////////////////////////////////////////////////////////////
   int64_t tair_client::ping(uint64_t server_id)
   {
       if (server_id == 0ul) {
          return 0L;
       }
     int ping_count = 10;
     int64_t total = 0;
     for (int i = 0; i < ping_count; ++i) {
       total += client_helper.ping(server_id);
     }
     return total / ping_count;
   }
   void tair_client::do_cmd_quit(VSTRING &param)
   {
      return ;
   }

   void tair_client::print_help(const char *cmd)
   {
      if (cmd == NULL || strcmp(cmd, "put") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : put key data [area] [expired]\n"
                 "DESCRIPTION: area   - namespace , default: 0\n"
                 "             expired- in seconds, default: 0,never expired\n"
            );
      }
      if (cmd == NULL || strcmp(cmd, "incr") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : incr key [count] [initValue] [area]\n"
                 "DESCRIPTION: initValue , default: 0\n"
            );
      }
      if (cmd == NULL || strcmp(cmd, "get") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : get key [area]\n"
            );
      }
      if (cmd == NULL || strcmp(cmd, "mget") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : mget key1 ... keyn area\n"
            );
      }
      if (cmd == NULL || strcmp(cmd, "remove") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : remove key [area]\n"
            );
      }
      if (cmd == NULL || strcmp(cmd, "mremove") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : mremove key1 ... keyn area\n"
            );
      }
      if (cmd == NULL || strcmp(cmd, "delall") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : stat\n"
                 "DESCRIPTION: get stat info"
            );
      }
      if (cmd == NULL || strcmp(cmd, "stat") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : delall area\n"
                 "DESCRIPTION: delete all data of [area]"
            );
      }

      if (cmd == NULL || strcmp(cmd, "dump") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : dump dumpinfo.txt\n"
                 "DESCRIPTION: dumpinfo.txt is a config file of dump,syntax:\n"
                 "area start_time end_time,eg:"
                 "10 2008-12-09 12:08:07 2008-12-10 12:10:00\n"
            );
      }

      if (cmd == NULL || strcmp(cmd, "hide") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: hide [area] key\n"
            "DESCRIPTION: to hide one item\n");
      }
      if (cmd == NULL || strcmp(cmd, "gethidden") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: gethidden [area] key\n"
            "DESCRIPTION: to get one hidden item\n");
      }
      if (cmd == NULL || strcmp(cmd, "pput") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: pput [area] pkey skey value\n"
            "DESCRIPTION: to put one item with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "pget") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: pget [area] pkey skey\n"
            "DESCRIPTION: to get one item with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "premove") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: premove [area] pkey skey\n"
            "DESCRIPTION: to remove one item with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "premoves") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: premoves area pkey skey1...skeyn\n"
            "DESCRIPTION: to remove multiple items with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "pgethidden") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: pgethidden [area] pkey skey\n"
            "DESCRIPTION: to get one hidden item with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "phide") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: phide [area] pkey skey\n"
            "DESCRIPTION: to hide one item with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "phides") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: phides area pkey skey1...skeyn\n"
            "DESCRIPTION: to hide multiple items with prefix\n");
      }
      if (cmd == NULL || strcmp(cmd, "flowlimit") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: flowlimit ns lower upper type, example flowlimit 123 30000 40000 ops\n"
            "DESCRIPTION: set or view flow limit bound\n");
      }
      if (cmd == NULL || strcmp(cmd, "flowrate") == 0) {
        fprintf(stderr,
            "------------------------------------------------\n"
            "SYNOPSIS: flowrate ns \n"
            "DESCRIPTION: view flow rate\n");
      }

      if (cmd == NULL || strcmp(cmd, "gettmpdownsvr") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : gettmpdownsvr group1 group2...\n"
                 "DESCRIPTION: get tmp down servers of group(s)\n"
                 "\tgroup[n]: groupnames of which to get status\n"
            );
      }

      if (cmd == NULL || strcmp(cmd, "resetserver") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : resetserver group [ds_addr ds_addr]\n"
                 "DESCRIPTION: clear the all or some specified by `ds_addr down server in group, namely 'tmp_down_server' in group.conf\n"
                 "\tgroup: groupname to reset\n"
                 "\tds_addr: dataserver to reset\n"
            );
      }

      if (cmd == NULL || strcmp(cmd, "set_migrate_wait") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : set_migrate_wait time_ms [ds_addr]\n"
                 "DESCRIPTION: set migrate wait  time ms to all or one specified by `ds_addr\n"
                 "\ttime_ms : time in ms unit\n"
                 "\tds_addr : specified dataserver address\n"
            );
      }

      fprintf(stderr, "\n");
   }

   void tair_client::do_cmd_help(VSTRING &param)
   {
      if (param.size() == 0U) {
         print_help(NULL);
      } else {
         print_help(param[0]);
      }
      return;
   }

   char *tair_client::canonical_key(char *key, char **akey, int *size)
   {
      char *pdata = key;
      if (key_format == 1) { // java
         *size = strlen(key)+2;
         pdata = (char*)malloc(*size);
         pdata[0] = '\0';
         pdata[1] = '\4';
         memcpy(pdata+2, key, strlen(key));
         *akey = pdata;
      } else if (key_format == 2) { // raw
         pdata = (char*)malloc(strlen(key)+1);
         util::string_util::conv_raw_string(key, pdata, size);
         *akey = pdata;
      } else {
         *size = strlen(key)+1;
      }

      return pdata;
   }

   void tair_client::do_cmd_put(VSTRING &param)
   {
      if (param.size() < 2U || param.size() > 4U) {
         print_help("put");
         return;
      }
      int area = default_area;
      int expired = 0;
      if (param.size() > 2U) area = atoi(param[2]);
      if (param.size() > 3U) expired = atoi(param[3]);

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      data_entry data(param[1], false);

      int ret = client_helper.put(area, key, data, expired, 0);
      fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

      if (akey) free(akey);
   }

   void tair_client::do_cmd_addcount(VSTRING &param)
   {
      if (param.size() < 1U) {
         print_help("incr");
         return ;
      }
      int count = 1;
      int initValue = 0;
      int area = default_area;
      if (param.size() > 1U) count = atoi(param[1]);
      if (param.size() > 2U) initValue = atoi(param[2]);
      if (param.size() > 3U) area = atoi(param[3]);

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      // put
      int retCount = 0;
      int ret = client_helper.add_count(area, key, count, &retCount, initValue);

      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "add failed:%d,%s.\n", ret,client_helper.get_error_msg(ret));
      } else {
         fprintf(stderr, "retCount: %d\n", retCount);
      }
      if (akey) free(akey);

      return;
   }

   void tair_client::do_cmd_get(VSTRING &param)
   {
      if (param.size() < 1U || param.size() > 2U ) {
         print_help("get");
         return;
      }
      int area = default_area;
      if (param.size() >= 2U) {
         char *p=param[1];
         if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p ++;
         }
         area |= atoi(p);
      }
      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      data_entry *data = NULL;

      // get
      int ret = client_helper.get(area, key, data);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "get failed: %s.\n",client_helper.get_error_msg(ret));
      } else if (data != NULL) {
         char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
         fprintf(stderr, "KEY: %s, LEN: %d\n raw data: %s, %s\n", param[0], data->get_size(), data->get_data(), p);
         free(p);
         delete data;
      }
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_mget(VSTRING &param)
   {
      if (param.size() < 2U) {
         print_help("mget");
         return;
      }
      int area = default_area;
      char *p=param[param.size() -1];
      if (*p == 'n') {
        area |= TAIR_FLAG_NOEXP;
        p ++;
      }
      area |= atoi(p);
      fprintf(stderr, "mget area: %d\n", area);

      vector<data_entry*> keys;
      for (int i = 0; i < static_cast<int>(param.size() - 1); ++i)
      {
        char *akey = NULL;
        int pkeysize = 0;
        fprintf(stderr, "mget key index: %u, key: %s\n", i, param[i]);
        char *pkey = canonical_key(param[i], &akey, &pkeysize);
        data_entry* key = new data_entry(pkey, pkeysize, false);
        keys.push_back(key);
        if (akey) free(akey);
      }
      tair_keyvalue_map datas;

      // mget
      int ret = client_helper.mget(area, keys, datas);
      if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_PARTIAL_SUCCESS)
      {
        tair_keyvalue_map::iterator mit = datas.begin();
        for ( ; mit != datas.end(); ++mit)
        {
          char *key = util::string_util::conv_show_string(mit->first->get_data(), mit->first->get_size());
          char *data = util::string_util::conv_show_string(mit->second->get_data(), mit->second->get_size());
          fprintf(stderr, "KEY: %s, RAW VALUE: %s, VALUE: %s, LEN: %d\n",
              key, mit->second->get_data(), data, mit->second->get_size());
          free(key);
          free(data);
        }
        fprintf(stderr, "get success, ret: %d.\n", ret);
      }
      else
      {
         fprintf(stderr, "get failed: %s, ret: %d.\n", client_helper.get_error_msg(ret), ret);
      }

      vector<data_entry*>::iterator vit = keys.begin();
      for ( ; vit != keys.end(); ++vit)
      {
        delete *vit;
        (*vit) = NULL;
      }
      tair_keyvalue_map::iterator kv_mit = datas.begin();
      for ( ; kv_mit != datas.end(); )
      {
        data_entry* key = kv_mit->first;
        data_entry* value = kv_mit->second;
        datas.erase(kv_mit++);
        delete key;
        key = NULL;
        delete value;
        value = NULL;
      }
      return ;
   }

// remove
   void tair_client::do_cmd_remove(VSTRING &param)
   {
      if (param.size() < 2U) {
         print_help("remove");
         return;
      }

      int area = default_area;
      if (param.size() >= 2U) area = atoi(param[1]);

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      int ret = client_helper.remove(area, key);
      fprintf(stderr, "remove: %s.\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }

  const char * FlowStatusStr(tair::stat::FlowStatus status) {
    switch (status) {
     case tair::stat::UP:
      return "UP";
     case tair::stat::KEEP:
      return "KEEP";
     case tair::stat::DOWN:
      return "DOWN";
    }
    return "UNKNOW";
  }

  void tair_client::do_cmd_get_flow_rate(VSTRING &param)
  {
    if (param.size() < 1U) {
      print_help("flowrate"); 
      return ;
    }
    int ns = atoi(param[0]);

    set<uint64_t> servers;
    client_helper.get_servers(servers);
    size_t success = 0;
    for (set<uint64_t>::iterator iter = servers.begin(); iter != servers.end(); ++iter) {
      uint64_t addr = *iter;
      tair::stat::Flowrate rate;
      int ret = client_helper.get_flow(addr, ns, rate);

      if (ret == 0) {
        fprintf(stderr, "success %s \tns:%d \tin:%d,%s \tout:%d,%s \tops:%d,%s \tstatus:%s\n", 
            tbsys::CNetUtil::addrToString(addr).c_str(), ns,
            rate.in, FlowStatusStr(rate.in_status),
            rate.out, FlowStatusStr(rate.out_status),
            rate.ops, FlowStatusStr(rate.ops_status),
            FlowStatusStr(rate.status));
        success++;
      } else {
        fprintf(stderr, "fail %s \terr:%d\n", tbsys::CNetUtil::addrToString(addr).c_str(), ret);
      }
    }
    if (success == servers.size()) {
      fprintf(stderr, "all success\n");
    } else if (success == 0) {
      fprintf(stderr, "all fail\n");
    } else {
      fprintf(stderr, "part success\n");
    }
  }

  void tair_client::do_cmd_set_flow_limit_bound(VSTRING &param)
  {
    if (param.size() < 4U) {
      print_help("flowlimit"); 
      return ;
    }
    int ns = atoi(param[0]);
    int lower = atoi(param[1]);
    int upper = atoi(param[2]);

    char *strtemp = NULL;
    int typestr_len = 0;
    char *typestr = canonical_key(param[3], &typestr, &typestr_len);
    tair::stat::FlowType type = tair::stat::IN;
    if (strncmp("in", typestr, 2) == 0)
      type = tair::stat::IN;
    else if (strncmp("out", typestr, 3) == 0)
      type = tair::stat::OUT;
    else if (strncmp("ops", typestr, 3) == 0)
      type = tair::stat::OPS;
    else {
      fprintf(stderr, "unknow type:%s just support [in, out, ops]", typestr);
      return ;
    }
    set_flow_limit_bound(ns, lower, upper, type);
    if (strtemp != NULL)
      free(strtemp);
  }

  const char * FlowTypeStr(tair::stat::FlowType type)
  {
    switch (type) {
      case tair::stat::IN:
        return "in";
      case tair::stat::OUT:
        return "out";
      case tair::stat::OPS:
        return "ops";
    }
    return "unknow";
  }

  void tair_client::set_flow_limit_bound(int ns, int lower, int upper, tair::stat::FlowType type)
  {
    set<uint64_t> servers;
    client_helper.get_servers(servers);
    size_t success = 0;
    for (set<uint64_t>::iterator iter = servers.begin(); iter != servers.end(); ++iter) {
      uint64_t addr = *iter;
      int local_ns = ns;
      int local_lower = lower;
      int local_upper = upper;
      tair::stat::FlowType local_type = type;
      int ret = client_helper.set_flow_limit_bound(addr, local_ns, 
          local_lower, 
          local_upper, 
          local_type);

      if (ret == 0) {
        fprintf(stderr, "success %s \tns:%d \tlower:%d \tupper:%d \ttype:%s\n", 
            tbsys::CNetUtil::addrToString(addr).c_str(),
            local_ns, local_lower, local_upper, FlowTypeStr(local_type));
        success++;
      } else {
        fprintf(stderr, "fail %s \terr:%d\n", tbsys::CNetUtil::addrToString(addr).c_str(), ret);
      }
    }
    if (success == servers.size()) {
      fprintf(stderr, "all success\n");
    } else if (success == 0) {
      fprintf(stderr, "all fail\n");
    } else {
      fprintf(stderr, "part success\n");
    }
  }

   void tair_client::do_cmd_mremove(VSTRING &param)
   {
      if (param.size() < 2U ) {
         print_help("mremove");
         return;
      }
      int area = default_area;
      char *p=param[param.size() -1];
      if (*p == 'n') {
        area |= TAIR_FLAG_NOEXP;
        p ++;
      }
      area |= atoi(p);
      fprintf(stderr, "mremove area: %d\n", area);

      vector<data_entry*> keys;
      for (int i = 0; i < static_cast<int>(param.size() - 1); ++i)
      {
        char *akey = NULL;
        int pkeysize = 0;
        fprintf(stderr, "mremove key index: %u, key: %s\n", i, param[i]);
        char *pkey = canonical_key(param[i], &akey, &pkeysize);
        data_entry* key = new data_entry(pkey, pkeysize, false);
        keys.push_back(key);
        if (akey) free(akey);
        //todo delete key
      }

      int ret = client_helper.mdelete(area, keys);
      fprintf(stderr, "mremove: %s, ret: %d\n", client_helper.get_error_msg(ret), ret);
      vector<data_entry*>::iterator vit = keys.begin();
      for ( ; vit != keys.end(); ++vit)
      {
        delete *vit;
        (*vit) = NULL;
      }
      return ;
   }

   void tair_client::do_cmd_stat(VSTRING &param)
   {
      if (is_data_server) {
         fprintf(stderr, "direct connect to ds, can not use stat\n");
         return;
      }

      map<string, string> out_info;
      map<string, string>::iterator it;
      string group = group_name;
      client_helper.query_from_configserver(request_query_info::Q_AREA_CAPACITY, group, out_info);
      fprintf(stderr,"%20s %20s\n","area","quota");
      for (it=out_info.begin(); it != out_info.end(); it++) {
         fprintf(stderr,"%20s %20s\n", it->first.c_str(), it->second.c_str());
      }
      fprintf(stderr,"\n");

      fprintf(stderr,"%20s %20s\n","server","status");
      client_helper.query_from_configserver(request_query_info::Q_DATA_SEVER_INFO, group, out_info);
      for (it=out_info.begin(); it != out_info.end(); it++) {
         fprintf(stderr,"%20s %20s\n", it->first.c_str(), it->second.c_str());
      }
      fprintf(stderr,"\n");

      fprintf(stderr,"%20s %20s\n","server","ping");
       set<uint64_t> servers;
       client_helper.get_servers(servers);
       for (set<uint64_t>::iterator it = servers.begin(); it != servers.end(); ++it) {
         int64_t ping_time = ping(*it);
         fprintf(stderr,"%20s %20lf\n", tbsys::CNetUtil::addrToString(*it).c_str(), ping_time / 1000.);
       }
      fprintf(stderr,"\n");

      for (it=out_info.begin(); it != out_info.end(); it++) {
         map<string, string> out_info2;
         map<string, string>::iterator it2;
           uint64_t server_id = tbsys::CNetUtil::strToAddr(it->first.c_str(), 0);
         client_helper.query_from_configserver(request_query_info::Q_STAT_INFO, group, out_info2, server_id);
         for (it2=out_info2.begin(); it2 != out_info2.end(); it2++) {
            fprintf(stderr,"%s : %s %s\n",it->first.c_str(), it2->first.c_str(), it2->second.c_str());
         }
      }
      map<string, string> out_info2;
      map<string, string>::iterator it2;
      client_helper.query_from_configserver(request_query_info::Q_STAT_INFO, group, out_info2, 0);
      for (it2=out_info2.begin(); it2 != out_info2.end(); it2++) {
         fprintf(stderr,"%s %s\n", it2->first.c_str(), it2->second.c_str());
      }

   }


   void tair_client::do_cmd_remove_area(VSTRING &param)
   {
      if (param.size() != 1U) {
         print_help("delall");
         return;
      }
      int area = atoi(param[0]);
      if(area < 0){
         return;
      }
      // remove
      int ret = client_helper.remove_area(area);
      fprintf(stderr, "removeArea: area:%d,%s\n", area,client_helper.get_error_msg(ret));
      return;
   }

   void tair_client::do_cmd_dump_area(VSTRING &param)
   {
      if (param.size() != 1U) {
         print_help("dump");
         return;
      }

      FILE *fp = fopen(param[0],"r");
      if (fp == NULL){
         fprintf(stderr,"open file %s failed",param[0]);
         return;
      }

      char buf[1024];
      struct tm start_time;
      struct tm end_time;
      set<dump_meta_info> dump_set;
      while(fgets(buf,sizeof(buf),fp) != NULL){
         dump_meta_info info;
         if (sscanf(buf,"%d %4d-%2d-%2d %2d:%2d:%2d %4d-%2d-%2d %2d:%2d:%2d",\
                    &info.area,\
                    &start_time.tm_year,&start_time.tm_mon,\
                    &start_time.tm_mday,&start_time.tm_hour,\
                    &start_time.tm_min,&start_time.tm_sec,
                    &end_time.tm_year,&end_time.tm_mon,\
                    &end_time.tm_mday,&end_time.tm_hour,\
                    &end_time.tm_min,&end_time.tm_sec) != 13){
            fprintf(stderr,"syntax error : %s",buf);
            continue;
         }

         start_time.tm_year -= 1900;
         end_time.tm_year -= 1900;
         start_time.tm_mon -= 1;
         end_time.tm_mon -= 1;

         if (info.area < -1 || info.area >= TAIR_MAX_AREA_COUNT){
            fprintf(stderr,"ilegal area");
            continue;
         }

         time_t tmp_time = -1;
         if ( (tmp_time = mktime(&start_time)) == static_cast<time_t>(-1)){
            fprintf(stderr,"incorrect time");
            continue;
         }
         info.start_time = static_cast<uint32_t>(tmp_time);

         if ( (tmp_time = mktime(&end_time)) == static_cast<time_t>(-1)){
            fprintf(stderr,"incorrect time");
            continue;
         }
         info.end_time = static_cast<uint32_t>(tmp_time);

         if (info.start_time < info.end_time){
            std::swap(info.start_time,info.end_time);
         }
         dump_set.insert(info);
      }
      fclose(fp);

      if (dump_set.empty()){
         fprintf(stderr,"what do you want to dump?");
         return;
      }

      // dump
      int ret = client_helper.dump_area(dump_set);
      fprintf(stderr, "dump : %s\n",client_helper.get_error_msg(ret));
      return;
   }

   void tair_client::do_cmd_hide(VSTRING &params)
   {
     if (params.size() != 2) {
       print_help("hide");
       return ;
     }
     int area = atoi(params[0]);
     data_entry key;
     key.set_data(params[1], strlen(params[1]) + 1);
     fprintf(stderr, "area: %d, key: %s\n", area, params[1]);
     int ret = client_helper.hide(area, key);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }
   void tair_client::do_cmd_set_migrate_wait_ms(VSTRING &param)
   {
     do_cmd_op_ds_or_not(param, "set_migrate_wait", TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS, 1);
   }

   void tair_client::do_cmd_get_hidden(VSTRING &params)
   {
     int area = 0;
     const char *kstr = NULL;
     if (params.size() == 2) {
       area = atoi(params[0]);
       kstr = params[1];
     } else if (params.size() == 1) {
       area = 0;
       kstr = params[0];
     } else {
       print_help("gethidden");
       return ;
     }
     data_entry key;
     key.set_data(kstr, strlen(kstr) + 1);
     data_entry *value = NULL;
     int ret = client_helper.get_hidden(area, key, value);
     if (value != NULL) {
       char *p = util::string_util::conv_show_string(value->get_data(), value->get_size());
       fprintf(stderr, "key: %s, len: %d\n", key.get_data(), key.get_size());
       fprintf(stderr, "value: %s|%s, len: %d\n", value->get_data(), p, value->get_size());
       free(p);
       delete value;
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }

   void tair_client::do_cmd_prefix_put(VSTRING &params)
   {
     int area = 0;
     const char *pstr = NULL,
                *sstr = NULL,
                *vstr = NULL;
     if (params.size() == 4) {
       area = atoi(params[0]);
       pstr = params[1];
       sstr = params[2];
       vstr = params[3];
     } else if (params.size() == 3) {
       area = 0;
       pstr = params[0];
       sstr = params[1];
       vstr = params[2];
     } else {
       print_help("pput");
       return ;
     }
     data_entry pkey,
                skey,
                value;
     pkey.set_data(pstr, strlen(pstr) + 1);
     skey.set_data(sstr, strlen(sstr) + 1);
     value.set_data(vstr, strlen(vstr) + 1);

     int ret = client_helper.prefix_put(area, pkey, skey, value, 0, 0);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }

   void tair_client::do_cmd_prefix_get(VSTRING &params)
   {
     int area = 0;
     const char *pstr = NULL,
                *sstr = NULL;
     if (params.size() == 3) {
       area = atoi(params[0]);
       pstr = params[1];
       sstr = params[2];
     } else if (params.size() == 2) {
       area = 0;
       pstr = params[0];
       sstr = params[1];
     } else {
       print_help("pget");
       return ;
     }

     data_entry pkey,
                skey;
     pkey.set_data(pstr, strlen(pstr) + 1);
     skey.set_data(sstr, strlen(sstr) + 1);
     data_entry *value = NULL;

     int ret = client_helper.prefix_get(area, pkey, skey, value);
     if (value != NULL) {
       char *p = util::string_util::conv_show_string(value->get_data(), value->get_size());
       fprintf(stderr, "pkey: %s, len: %d\n", pkey.get_data(), pkey.get_size());
       fprintf(stderr, "skey: %s, len: %d\n", skey.get_data(), skey.get_size());
       fprintf(stderr, "value: %s|%s, len %d\n", value->get_data(), p, value->get_size());
       free(p);
       delete value;
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }

   void tair_client::do_cmd_prefix_remove(VSTRING &params)
   {
     int area = 0;
     const char *pstr = NULL,
                *sstr = NULL;
     if (params.size() == 3) {
       area = atoi(params[0]);
       pstr = params[1];
       sstr = params[2];
     } else if (params.size() == 2) {
       area = 0;
       pstr = params[0];
       sstr = params[1];
     }
     data_entry pkey,
                skey;
     pkey.set_data(pstr, strlen(pstr) + 1);
     skey.set_data(sstr, strlen(sstr) + 1);

     int ret = client_helper.prefix_remove(area, pkey, skey);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }

   void tair_client::do_cmd_prefix_removes(VSTRING &params)
   {
     if (params.size() < 3) {
       print_help("premoves");
       return ;
     }
     int area = atoi(params[0]);
     data_entry pkey;
     pkey.set_data(params[1], strlen(params[1]) + 1);
     tair_dataentry_set skey_set;
     for (size_t i = 2; i < params.size(); ++i) {
       data_entry *skey = new data_entry();
       skey->set_data(params[i], strlen(params[i]) + 1);
       skey_set.insert(skey);
     }
     key_code_map_t key_code_map;
     int ret = client_helper.prefix_removes(area, pkey, skey_set, key_code_map);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       key_code_map_t::iterator itr = key_code_map.begin();
       while (itr != key_code_map.end()) {
         data_entry *skey = itr->first;
         int ret = itr->second;
         fprintf(stderr, "failed key: %s, len: %d, code: %d, msg: %s\n",
             skey->get_data(), skey->get_size(), ret, client_helper.get_error_msg(ret));
         ++itr;
         delete skey;
       }
     }
   }

   void tair_client::do_cmd_prefix_get_hidden(VSTRING &params)
   {
     int area = 0;
     const char *pstr = NULL,
                *sstr = NULL;
     if (params.size() == 3) {
       area = atoi(params[0]);
       pstr = params[1];
       sstr = params[2];
     } else if (params.size() == 2) {
       area = 0;
       pstr = params[0];
       sstr = params[1];
     } else {
       print_help("pget");
       return ;
     }

     data_entry pkey,
                skey;
     pkey.set_data(pstr, strlen(pstr) + 1);
     skey.set_data(sstr, strlen(sstr) + 1);
     data_entry *value = NULL;

     int ret = client_helper.prefix_get_hidden(area, pkey, skey, value);
     if (value != NULL) {
       char *p = util::string_util::conv_show_string(value->get_data(), value->get_size());
       fprintf(stderr, "code: %d, msg: %s\n", ret, client_helper.get_error_msg(ret));
       fprintf(stderr, "pkey: %s, len: %d\n", pkey.get_data(), pkey.get_size());
       fprintf(stderr, "skey: %s, len: %d\n", skey.get_data(), skey.get_size());
       fprintf(stderr, "value: %s|%s, len %d\n", value->get_data(), p, value->get_size());
       free(p);
       delete value;
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }

   void tair_client::do_cmd_prefix_hide(VSTRING &params)
   {
     int area = 0;
     const char *pstr = NULL,
                *sstr = NULL;
     if (params.size() == 3) {
       area = atoi(params[0]);
       pstr = params[1];
       sstr = params[2];
     } else if (params.size() == 2) {
       area = 0;
       pstr = params[0];
       sstr = params[1];
     }
     data_entry pkey,
                skey;
     pkey.set_data(pstr, strlen(pstr) + 1);
     skey.set_data(sstr, strlen(sstr) + 1);

     int ret = client_helper.prefix_hide(area, pkey, skey);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
     }
   }

   void tair_client::do_cmd_prefix_hides(VSTRING &params)
   {
     if (params.size() < 3) {
       print_help("phides");
       return ;
     }
     int area = atoi(params[0]);
     data_entry pkey;
     pkey.set_data(params[1], strlen(params[1]) + 1);
     tair_dataentry_set skey_set;
     for (size_t i = 2; i < params.size(); ++i) {
       data_entry *skey = new data_entry();
       skey->set_data(params[i], strlen(params[i]) + 1);
       skey_set.insert(skey);
     }
     key_code_map_t key_code_map;
     int ret = client_helper.prefix_hides(area, pkey, skey_set, key_code_map);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       key_code_map_t::iterator itr = key_code_map.begin();
       while (itr != key_code_map.end()) {
         data_entry *skey = itr->first;
         int ret = itr->second;
         fprintf(stderr, "failed key: %s, len: %d, code: %d, msg: %s\n",
             skey->get_data(), skey->get_size(), ret, client_helper.get_error_msg(ret));
         ++itr;
         delete skey;
       }
     }
   }

   void tair_client::do_cmd_gettmpdownsvr(VSTRING &params) {
     if (params.empty()) {
       print_help("gettmpdownsvr");
       return ;
     }
     vector<string> down_servers;
     std::vector<std::string> cmd_params(params.begin(), params.end());
     int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER, &cmd_params, &down_servers);
     if (TAIR_RETURN_SUCCESS == ret) {
       for (size_t i = 0; i < down_servers.size(); ++i) {
         fprintf(stderr, "\t%s\n", down_servers[i].c_str());
       }
     } else {
       fprintf(stderr, "failed with %d\n", ret);
     }
   }

   void tair_client::do_cmd_resetserver(VSTRING &params) {
     if (params.size() < 1) {
       print_help("resetserver");
       return ;
     }
     std::vector<std::string> cmd_params(params.begin(), params.end());
     int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_RESET_DS, &cmd_params, NULL);
     if (ret == TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "successful\n");
     } else {
       fprintf(stderr, "failed with %d\n", ret);
     }
   }

   void tair_client::do_cmd_stat_db(VSTRING &param)
   {
     do_cmd_op_ds_or_not(param, "stat_db", TAIR_SERVER_CMD_STAT_DB);
   }

   void tair_client::do_cmd_release_mem(VSTRING& param)
   {
     do_cmd_op_ds_or_not(param, "release_mem", TAIR_SERVER_CMD_RELEASE_MEM);
   }

   void tair_client::do_cmd_pause_gc(VSTRING& param)
   {
     do_cmd_op_ds_or_not(param, "pause_mem", TAIR_SERVER_CMD_PAUSE_GC);
   }

   void tair_client::do_cmd_resume_gc(VSTRING& param)
   {
     do_cmd_op_ds_or_not(param, "resume_mem", TAIR_SERVER_CMD_RESUME_GC);
   }

   void tair_client::do_cmd_set_config(VSTRING& param)
   {
     do_cmd_op_ds_or_not(param, "set_config", TAIR_SERVER_CMD_SET_CONFIG, 2);
   }

   void tair_client::do_cmd_op_ds_or_not(VSTRING &param, const char* cmd_str, ServerCmdType cmd_type, int base_param_size)
   {
     if (static_cast<int>(param.size()) < base_param_size || static_cast<int>(param.size()) > base_param_size + 1) {
       print_help(cmd_str);
       return ;
     }
     std::vector<std::string> cmd_params;
     for (int i = 0; i < base_param_size; ++i) {
       cmd_params.push_back(param[i]);
     }
     int ret = client_helper.op_cmd_to_ds(cmd_type, &cmd_params,
                                          static_cast<int>(param.size()) > base_param_size ? param[base_param_size] : NULL);
     if (ret != TAIR_RETURN_SUCCESS) {
       fprintf(stderr, "%s fail, ret: %d\n", cmd_str, ret);
     } else {
       fprintf(stderr, "%s success.\n", cmd_str);
     }
   }

} // namespace tair


/*-----------------------------------------------------------------------------
 *  main
 *-----------------------------------------------------------------------------*/

tair::tair_client _globalClient;
void sign_handler(int sig)
{
   switch (sig) {
      case SIGTERM:
      case SIGINT:
         _globalClient.cancel();
   }
}
int main(int argc, char *argv[])
{
   signal(SIGPIPE, SIG_IGN);
   signal(SIGHUP, SIG_IGN);
   signal(SIGINT, sign_handler);
   signal(SIGTERM, sign_handler);

   //TBSYS_LOGGER.setLogLevel("DEBUG");
   TBSYS_LOGGER.setLogLevel("ERROR");
   if (_globalClient.parse_cmd_line(argc, argv) == false) {
      return EXIT_FAILURE;
   }
   if (_globalClient.start()) {
      return EXIT_SUCCESS;
   } else {
      return EXIT_FAILURE;
   }
}
