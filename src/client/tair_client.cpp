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
      cmd_map["remove"] = &tair_client::do_cmd_remove;
      cmd_map["delall"] = &tair_client::do_cmd_remove_area;
      cmd_map["dump"] = &tair_client::do_cmd_dump_area;
      cmd_map["stat"] = &tair_client::do_cmd_stat;
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
         //done = client_helper.startup(server_id);
         done = false;
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
            if (fprintf(stderr, "TAIR> ") < 0) break;
            if (fgets(buffer, CMD_MAX_LEN, stdin) == NULL) {
               continue;
            }
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

///////////////////////////////////////////////////////////////////////////////////////////////////
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
      if (cmd == NULL || strcmp(cmd, "remove") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : remove key [area]\n"
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
                 "10 2008-12-09 12:08:07 2008-12-10 12:10:00"
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
         fprintf(stderr, "add failed:%s.\n", client_helper.get_error_msg(ret));
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
         fprintf(stderr, "KEY: %s, LEN: %d\n%s\n", param[0], data->get_size(), p);
         free(p);
         delete data;
      }
      if (akey) free(akey);
      return ;
   }
// remove
   void tair_client::do_cmd_remove(VSTRING &param)
   {
      if (param.size() != 1U && param.size() != 2U ) {
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
   
   
   void tair_client::do_cmd_stat(VSTRING &param) {

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

	for (it=out_info.begin(); it != out_info.end(); it++) {
		map<string, string> out_info2;
		map<string, string>::iterator it2;
		client_helper.query_from_configserver(request_query_info::Q_STAT_INFO, group, out_info2, tbsys::CNetUtil::strToAddr(it->first.c_str(), 0));
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
