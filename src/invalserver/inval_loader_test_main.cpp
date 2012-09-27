#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <string>
#include <ext/hash_map>
#include "inval_loader_test.hpp"


tair::InvalLoader* invalid_loader = NULL;
uint64_t tair::util::local_server_ip::ip = 0;
//~ signal handler
void sig_handler(int sig) {
  switch (sig) {
    case SIGTERM:
    case SIGINT:
      if (invalid_loader != NULL) {
        invalid_loader->stop();
      }
      break;
    case 40:
      TBSYS_LOGGER.checkFile();
      break;
    case 41:
    case 42:
      if (sig == 41) {
        TBSYS_LOGGER._level++;
      } else {
        TBSYS_LOGGER._level--;
      }
      log_error("log level changed to %d.", TBSYS_LOGGER._level);
      break;
  }
}
//~ print the help information
void print_usage(char *prog_name) {
  fprintf(stderr, "%s -f config_file\n"
      "    -f, --config_file  config file\n"
      "    -h, --help         show this info\n"
      "    -V, --version      show build time\n\n",
      prog_name);
}
//~ parse the command line
char *parse_cmd_line(int argc, char *const argv[], bool &failed_get_group_names, bool &failed_startup) {
  int opt;
  const char *optstring = "hVf:gs";
  struct option longopts[] = {
    {"config_file", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {"version", 0, NULL, 'V'},
    {"failed_abtain_group_names", 0, NULL, 'g'},
    {"failed_startup", 0, NULL, 's'},
    {0, 0, 0, 0}
  };
  failed_get_group_names = true;
  failed_startup = true;
  char *configFile = NULL;
  while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) {
    switch (opt) {
      case 'f':
        configFile = optarg;
        break;
      case 'g':
        failed_get_group_names = false;
        break;
      case 's':
        failed_startup = false;
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\nSVN: %s\n", __DATE__, __TIME__, TAIR_SVN_INFO);
        exit (1);
      case 'h':
        print_usage(argv[0]);
        exit (1);
    }
  }
  return configFile;
}
void logs_settings(const char *configfile)
{
  if (TBSYS_CONFIG.load(configfile)) {
    fprintf(stderr, "load ConfigFile %s failed.\n", configfile);
    return;
  }
  const char *logFile = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_LOG_FILE, "inval.log");
  char *p, dirpath[256];
  snprintf(dirpath, sizeof(dirpath), "%s", logFile);
  p = strrchr(dirpath, '/');
  if (p != NULL) *p = '\0';
  if (p != NULL && !tbsys::CFileUtil::mkdirs(dirpath)) {
    fprintf(stderr, "mkdirs %s failed.\n", dirpath);
    return;
  }
  const char *logLevel = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_LOG_LEVEL, "info");
  TBSYS_LOGGER.setLogLevel(logLevel);
  TBSYS_LOGGER.setMaxFileSize(1<<23);
}

int main(int argc, char **argv) {
  bool failed_abtain_group_names;
  bool failed_startup;
  const char *configfile = parse_cmd_line(argc, argv, failed_abtain_group_names, failed_startup);
  if (configfile == NULL) {
    print_usage(argv[0]);
    return 1;
  }
  logs_settings(configfile);

  signal(SIGPIPE, SIG_IGN);
  signal(SIGHUP, SIG_IGN);
  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  signal(40, sig_handler);
  signal(41, sig_handler);
  signal(42, sig_handler);

  invalid_loader = new tair::inval_loader_test(failed_abtain_group_names, failed_startup);
  invalid_loader->start();
  invalid_loader->wait();

  delete invalid_loader;

  return 0;
}
