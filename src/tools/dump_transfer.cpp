#include <getopt.h>
#include "tair_client_api.hpp"
#include <pthread.h>
#include <queue>
#include <string>
#include "define.hpp"
#include "data_entry.hpp"
#include "log.hpp"
using tair::common::data_entry;

class DumpTransfer: public tbsys::CDefaultRunnable
{
public:
    DumpTransfer();
    ~DumpTransfer();

    inline void set_config_server(const char *cs);
    inline void set_group(const char *group);
    inline void set_dump_file(const char *dump_file);
    inline void set_interval(int interval);

    bool init();
    void run(tbsys::CThread *thread, void *args);
    void stop();

private:
    struct kv_pair {
        kv_pair(data_entry *k = NULL, data_entry *v = NULL) {
            key = k;
            value = v;
        }
        ~kv_pair() {
            if (key != NULL)
                delete key;
            if (value != NULL)
                delete value;
        }

        data_entry *key,
                   *value;
        uint32_t key_len,
                 value_len;
        uint32_t area,
                 mdate,
                 edate;
        uint16_t version;
    };

private:
    void parse_dump_file();
    int put(kv_pair *kv);

private:
    std::queue<kv_pair*> taskq_;
    int max_qsize_;
    tbsys::CThreadCond qcond_;
    std::string dump_file_;
    FILE *dump_fd_;

    std::string config_server_;
    std::string group_name_;
    tair::tair_client_api *client_;

    int interval_;
    bool finished_;
};

bool DumpTransfer::init()
{
    client_ = new tair::tair_client_api();
    if (client_->startup(config_server_.c_str(), NULL, group_name_.c_str()) == false) {
        log_error("startup client_ failed.");
        delete client_;
        client_ = NULL;
        return false;
    }
    return true;
}

void DumpTransfer::set_config_server(const char *cs)
{
    config_server_ = cs;
}

void DumpTransfer::set_group(const char *group)
{
    group_name_ = group;
}

void DumpTransfer::set_dump_file(const char *dump_file)
{
    dump_file_ = dump_file;
}

void DumpTransfer::set_interval(int interval)
{
    interval_ = interval;
}

DumpTransfer::DumpTransfer()
{
    dump_fd_ = NULL;
    interval_ = 0;
    max_qsize_ = 1000;
    finished_ = false;
    client_ = NULL;
}

DumpTransfer::~DumpTransfer() {
    if (!_stop) {
        stop();
    }
    if (client_ != NULL) {
        client_->close();
        delete client_;
        client_ = NULL;
    }
    while (!taskq_.empty()) {
        kv_pair *kv = taskq_.front();
        delete kv;
        taskq_.pop();
    }
}

void DumpTransfer::run(tbsys::CThread *thread, void *args)
{
    int thread_idx = (int) ((long)args);

    if (thread_idx == 0) {
        parse_dump_file();
    } else {
        log_warn("thread %d start putting key", thread_idx);
        while (!_stop) {
            qcond_.lock();
            while (!_stop  && !finished_ && taskq_.size() == 0) {
                qcond_.wait();
            }
            if (_stop || (finished_ && taskq_.empty())) {
                qcond_.unlock();
                break;
            }

            kv_pair *kv = taskq_.front();
            taskq_.pop();
            qcond_.unlock();
            qcond_.signal();

            int ret;
            if ((ret = put(kv)) != TAIR_RETURN_SUCCESS) {
                log_error("thread %d put failed, ret: %d", thread_idx, ret);
                if (ret != TAIR_RETURN_VERSION_ERROR) {
                    while (!_stop) {
                        log_warn("thread %d retry to put", thread_idx);
                        if ((ret = put(kv)) == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_VERSION_ERROR) {
                            break;
                        } else {
                            log_warn("thread %d retry putting failed.", thread_idx);
                        }
                        if (interval_ != 0) {
                            usleep(interval_);
                        }
                    }
                } else {
                    log_error("thread %d put failed, version error.");
                }
            }
            delete kv;
            if (interval_ != 0) {
                usleep(interval_);
            }
        }
    }
}

void DumpTransfer::parse_dump_file()
{
    dump_fd_ = fopen(dump_file_.c_str(), "r");
    if (dump_fd_ == NULL) {
        log_error("open %s failed: %m.", dump_file_.c_str());
        return ;
    }
    const int kbuf_size = 1<<10;
    const int vbuf_size = 1<<20;
    char *kbuf = new char[kbuf_size];
    char *vbuf = new char[vbuf_size];

    uint32_t cdate;
    while (!_stop) {
        kv_pair *kv = new kv_pair();
        int rd_size;
        rd_size = fread(&kv->area, sizeof(uint32_t), 1, dump_fd_);
        if (rd_size == 0 && feof(dump_fd_)) {
            log_error("read %s complete.", dump_file_.c_str());
            finished_ = true;
            delete kv;
            break;
        }
        fread(&kv->version, sizeof(uint16_t), 1, dump_fd_);
        fread(&cdate, sizeof(uint32_t), 1, dump_fd_);
        fread(&kv->mdate, sizeof(uint32_t), 1, dump_fd_);
        fread(&kv->edate, sizeof(uint32_t), 1, dump_fd_);
        fread(&kv->key_len, sizeof(uint32_t), 1, dump_fd_);
        assert(kv->key_len <= kbuf_size);
        fread(kbuf, sizeof(char), kv->key_len, dump_fd_);
        fread(&kv->value_len, sizeof(uint32_t), 1, dump_fd_);
        assert(kv->value_len <= vbuf_size);
        fread(vbuf, sizeof(char), kv->value_len, dump_fd_);

        char *key = new char[kv->key_len];
        memcpy(key, kbuf, kv->key_len);
        kv->key = new data_entry();
        kv->key->set_alloced_data(key, kv->key_len);
        char *value = new char[kv->value_len];
        memcpy(value, vbuf, kv->value_len);
        kv->value = new data_entry();
        kv->value->set_alloced_data(value, kv->value_len);

        qcond_.lock();
        while (!_stop && taskq_.size() >= max_qsize_) {
            qcond_.wait();
        }
        if (_stop) {
            delete kv;
            break;
        }
        taskq_.push(kv);
        qcond_.unlock();
        qcond_.signal();
    }
    delete [] kbuf;
    delete [] vbuf;
    fclose(dump_fd_);
}

int DumpTransfer::put(kv_pair *kv)
{
    return client_->put(kv->area, *kv->key, *kv->value, kv->edate, kv->version);
}

void DumpTransfer::stop() {
    CDefaultRunnable::stop();
    client_->close();
}

void print_usage(char *prog_name);

DumpTransfer *transfer = NULL;

    int
main(int argc, char **argv)
{
    //~ parse the command line
    int opt;
    const char *optstring = "hvc:g:l:f:t:i:";
    struct option longopts[] = {
        {"config_server", 1, NULL, 'c'},
        {"group_name", 1, NULL, 'g'},
        {"log_file", 1, NULL, 'l'},
        {"dump_file", 1, NULL, 'f'},
        {"thread_count", 1, NULL, 't'},
        {"interval", 1, NULL, 'i'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'v'},
        {0, 0, 0, 0}
    };

    const char *config_server = NULL;
    const char *group_name = NULL;
    const char *log_file = "logs/dump_transfer.log";
    const char *dump_file = NULL;
    const char *thread_count = "53";
    const char *interval = "0";
    while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) {
        switch (opt) {
            case 'c':
                config_server = optarg;
                break;
            case 'g':
                group_name = optarg;
                break;
            case 'l':
                log_file = optarg;
                break;
            case 'f':
                dump_file = optarg;
                break;
            case 't':
                thread_count = optarg;
                break;
            case 'i':
                interval = optarg;
                break;
            case 'v':
                fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
                exit (1);
            case 'h':
                print_usage(argv[0]);
                exit (1);
        }
    }
    if (config_server == NULL || group_name == NULL || dump_file == NULL) {
        print_usage(argv[0]);
        return 1;
    }

    char *p, dirpath[256];
    snprintf(dirpath, sizeof(dirpath), "%s", log_file);
    p = strrchr(dirpath, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dirpath)) {
        fprintf(stderr, "mkdirs %s failed.\n", dirpath);
        return 1;
    }

    TBSYS_LOGGER.setFileName(log_file);
    TBSYS_LOGGER.setLogLevel("info");

    transfer = new DumpTransfer();
    transfer->set_config_server(config_server);
    transfer->set_group(group_name);
    transfer->set_dump_file(dump_file);
    transfer->set_interval(atoi(interval));
    transfer->setThreadCount(atoi(thread_count));

    if (!transfer->init()) {
        delete transfer;
        transfer = NULL;
    }

    transfer->start();
    transfer->wait();
    log_info("transfer complete.");

    return 0;
}


//~ print the help information
void print_usage(char *prog_name) {
    fprintf(stderr, "%s -c configserver -g groupname -f dump_file\n"
            "    -c, --config_server  config server\n"
            "    -g, --group_name  group name\n"
            "    -l, --log_file  log file\n"
            "    -f, --dump_file  dump data file\n"
            "    -t, --thread_count  thread count working\n"
            "    -i, --interval  interval(us) between putings\n"
            "    -h, --help         show this message\n"
            "    -v, --version      show version\n\n",
            prog_name);
}
