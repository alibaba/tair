#include <getopt.h>
#include <easy_io.h>
#include <easy_string.h>
#include <easy_http_handler.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <arpa/inet.h>
#include <linux/if.h>
#include <sys/ioctl.h>
#include <easy_socket.h>
#if HAVE_SYS_INOTIFY_H
#include <sys/inotify.h>
#endif

// 命令行参数结构
#define HTTP_STATUS_PATH_LEN 128
#define HTTP_STATUS_MAX_BUFFER 16384
typedef struct http_status_dirent_t {
    long                    d_ino;
    __kernel_off_t          d_off;
    unsigned short          d_reclen;
    char                    d_name[0];
} http_status_dirent_t;
typedef struct http_status_file_t {
    char                    *name;
    easy_hash_list_t        node;
} http_status_file_t;
typedef struct http_status_vip_t {
    uint64_t                addr;
    easy_hash_list_t        node;
} http_status_vip_t;
typedef struct cmdline_param {
    char                    host[64];
    char                    monitor_dir[HTTP_STATUS_PATH_LEN];
    char                    pidfile[HTTP_STATUS_PATH_LEN];
    int                     port;
    int                     daemon;
    uint64_t                qps;

    // watcher
    ev_timer                vip_watcher;
    ev_io                   dir_watcher;
    int                     ifd;

    // dir
    int                     dir_cnt;
    uint64_t                dir_code;
    easy_pool_t             *dir_pool;
    easy_hash_t             *dir_table;
    // vip
    int                     vip_cnt;
    uint64_t                vip_code;
    easy_pool_t             *vip_pool;
    easy_hash_t             *vip_table;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int http_status_on_process(easy_request_t *r);
static int http_status_vip_exist(char *vip);
static int http_status_file_exist(char *statusfile);
static int http_status_file_cmp(const void *a, const void *b);
static void http_status_read_vip(struct ev_loop *loop, ev_timer *w, int revents);
static void http_status_read_dir(struct ev_loop *loop, ev_io *w, int revents);
static void print_usage(char *prog_name);
static void daemonize(const char *pidfile);
static int http_status_start_notify();

/**
 * 程序入口
 */
int main(int argc, char **argv)
{
    easy_listen_t           *listen;
    easy_io_handler_pt      io_handler;
    int                     ret;

    // default
    memset(&cp, 0, sizeof(cmdline_param));
    lnprintf(cp.monitor_dir, HTTP_STATUS_PATH_LEN, "/var/run/http_status");
    lnprintf(cp.pidfile, HTTP_STATUS_PATH_LEN, "/var/run/http_status.pid");
    cp.port = 8001;
    cp.ifd = -1;

    // parse cmd line
    if (parse_cmd_line(argc, argv, &cp) == EASY_ERROR)
        return EASY_ERROR;

    // 检查必需参数
    if (cp.port == 0) {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    // daemon
    if (cp.daemon)
        daemonize(cp.pidfile);

    // 对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(1)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    // 为监听端口设置处理函数，并增加一个监听端口
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = http_status_on_process;

    if ((listen = easy_io_add_listen(NULL, cp.port, &io_handler)) == NULL) {
        easy_error_log("easy_io_add_listen error, port: %d, %s\n",
                       cp.port, strerror(errno));
        return EASY_ERROR;
    } else {
        easy_error_log("listen start, port = %d\n", cp.port);
    }

    // 加入notify
    http_status_start_notify();

    // 起线程并开始
    if (easy_io_start()) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // 等待线程退出
    ret = easy_io_wait();
    easy_io_destroy();

    if (cp.dir_pool) easy_pool_destroy(cp.dir_pool);

    if (cp.vip_pool) easy_pool_destroy(cp.vip_pool);

    if (cp.ifd >= 0) close(cp.ifd);

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s [-p port] [-i ip] [-d] [-m monitor_dir]\n"
            "    -p, --port              port to listen, default: 8001\n"
            "    -i, --ip                ip address to bind, default: 0.0.0.0\n"
            "    -d, --daemon            fork the daemon into the background\n"
            "    -m, --monitor           monitor directory, default: /var/run/http_status\n"
            "    --pidfile               pid file, default: /var/run/http_status.pid\n"
            "    -h, --help              display this help and exit\n"
            "    -V, --version           version and build time\n\n"
            "eg: %s -p 5000\n\n", prog_name, prog_name);
}

/**
 * 解析命令行
 */
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp)
{
    int                     opt;
    const char              *opt_string = "hVp:i:m:d1:";
    struct option           long_opts[] = {
        {"port", 1, NULL, 'p'},
        {"ip", 1, NULL, 'i'},
        {"daemon", 0, NULL, 'd'},
        {"monitor_dir", 1, NULL, 'm'},
        {"pidfile", 1, NULL, '1'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {0, 0, 0, 0}
    };

    opterr = 0;

    while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
        switch (opt) {
        case 'p':
            cp->port = atoi(optarg);
            break;

        case 'i':
            lnprintf(cp->host, 64, optarg);
            break;

        case 'd':
            cp->daemon = 1;
            break;

        case 'm':
            if (realpath(optarg, cp->monitor_dir) == NULL) {
                cp->monitor_dir[0] = '\0';
                fprintf(stderr, "directory: %s not found.\n", optarg);
                return EASY_ERROR;
            }

            break;

        case '1':
            lnprintf(cp->pidfile, HTTP_STATUS_PATH_LEN, optarg);
            break;

        case 'V':
            fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
            return EASY_ERROR;

        case 'h':
            print_usage(argv[0]);
            return EASY_ERROR;

        default:
            break;
        }
    }

    return EASY_OK;
}

/**
 * 处理函数
 */
static int http_status_on_process(easy_request_t *r)
{
    char                        *service, *port, *vip;
    char                        *k, *v, *nk;
    char                        statusfile[HTTP_STATUS_PATH_LEN];
    easy_http_request_t         *p;

    r->opacket = p = (easy_http_request_t *)r->ipacket;

    // qps
    if (p->str_path.len == 4 && memcmp(p->str_path.data, "/qps", 4) == 0) {
        easy_io_thread_t        *ioth = (easy_io_thread_t *)easy_thread_pool_index(easy_io_var.io_thread_pool, 0);
        double                  qps = cp.qps / (ev_now(ioth->loop) - easy_io_var.start_time);
        easy_http_request_printf(p, "QPS: %f\n", qps);
        return EASY_OK;
    }

    // path
    easy_http_add_header(r->ms->pool, p->headers_out, "Cache-Control", "private");

    if (p->str_path.len != 7 || memcmp(p->str_path.data, "/status", 7)) {
        easy_buf_string_set(&p->status_line, "404 Not Found - No such function defined!");
        return EASY_OK;
    }

    // parse SERVICE, PORT, VIP
    service = "status.html";
    port = vip = NULL;
    k = p->str_query_string.data;

    while(k) {
        if ((nk = strchr(k, '&')) != NULL)
            (*nk ++) = '\0';

        if ((v = strchr(k, '=')) != NULL) {
            (*v ++) = '\0';
            easy_string_toupper(k);

            if (strcmp(k, "SERVICE") == 0)
                service = v;
            else if (strcmp(k, "PORT") == 0)
                port = v;
            else if (strcmp(k, "VIP") == 0)
                vip = v;
        }

        k = nk;
    }

    // 多端口
    if (port) {
        lnprintf(statusfile, HTTP_STATUS_PATH_LEN, "%s.%s", service, port);
        service = statusfile;
    }

    // 文件存在否?
    char                    *msg = "404 Not Found - Service not aviliable!";

    if (cp.dir_table && http_status_file_exist(service)) {
        if (vip) {
            if (http_status_vip_exist(vip)) {
                msg = "200 OK - Service active within VIP!";
            } else {
                msg = "404 Not Found - Service active but not in VIP!";
            }
        } else {
            msg = "200 OK - Service active!";
        }
    }

    cp.qps ++;
    easy_buf_string_set(&p->status_line, msg);
    //easy_buf_string_set(&p->output_line, msg);
    easy_http_request_printf(p, msg);

    return EASY_OK;
}

/**
 * http_status_file_cmp
 */
static int http_status_file_cmp(const void *a, const void *b)
{
    http_status_file_t      *f = (http_status_file_t *) b;
    return strcmp((char *)a, f->name);
}

/**
 * 文件存在否?
 */
static int http_status_file_exist(char *statusfile)
{
    uint64_t                key;
    key = easy_hash_code(statusfile, strlen(statusfile), 5);
    return easy_hash_find_ex(cp.dir_table, key, http_status_file_cmp, statusfile) ? 1 : 0;
}

/**
 * VIP存在否?
 */
static int http_status_vip_exist(char *vip)
{
    char                    *s, *e;
    uint64_t                ip;
    int                     ret = 0;

    if (cp.vip_table == NULL)
        return 0;

    s = vip;

    while(s) {
        if ((e = strchr(s, ',')) != NULL)
            (*e++) = '\0';

        if ((ip = inet_addr(s)) == INADDR_NONE)
            return 0;

        if (easy_hash_find(cp.vip_table, ip) == NULL)
            return 0;

        ret ++;
        s = e;
    }

    return ret;
}

/**
 * 检查目录及vip
 */
void http_status_check_dirvip(struct ev_loop *loop, ev_timer *w, int revents)
{
    static int              cnt = 0;
    http_status_read_dir(NULL, NULL, 0);

    if (cnt ++ % 30 == 0) http_status_read_vip(NULL, NULL, 0);
}

static int http_status_start_notify()
{
    easy_io_thread_t        *ioth;

#if HAVE_SYS_INOTIFY_H
    int                     fd, wd;

    fd = inotify_init();

    if (fd < 0) {
        easy_error_log("Fail to initialize inotify.\n");
        return EASY_ERROR;
    }

    wd = inotify_add_watch(fd, cp.monitor_dir, IN_MOVE | IN_CREATE | IN_DELETE | IN_UNMOUNT);

    if (wd < 0) {
        close(fd);
        easy_error_log("Can't add watch for %s.\n", cp.monitor_dir);
        return EASY_ERROR;
    }

    cp.ifd = fd;
    easy_socket_non_blocking(fd);

    // 定时检测VIP地址及目录
    ioth = (easy_io_thread_t *)easy_thread_pool_index(easy_io_var.io_thread_pool, 0);
    ev_io_init (&cp.dir_watcher, http_status_read_dir, fd, EV_READ);
    ev_io_start (ioth->loop, &cp.dir_watcher);
    ev_timer_init (&cp.vip_watcher, http_status_read_vip, 0., 60.0);
    ev_timer_start (ioth->loop, &cp.vip_watcher);
    easy_baseth_on_wakeup(ioth);
#else
    ioth = (easy_io_thread_t *)easy_thread_pool_index(easy_io_var.io_thread_pool, 0);
    ev_timer_init (&cp.vip_watcher, http_status_check_dirvip, 0., 2.0);
    ev_timer_start (ioth->loop, &cp.vip_watcher);
    easy_baseth_on_wakeup(ioth);
#endif

    // 先加载一次
    http_status_read_dir(NULL, NULL, 0);
    http_status_read_vip(NULL, NULL, 0);

    return EASY_OK;
}

/**
 * 检查文件是否存在
 */
static void http_status_read_dir(struct ev_loop *loop, ev_io *w, int revents)
{
    char                    buffer[HTTP_STATUS_MAX_BUFFER], *p;
    int                     fd, cnt, n;
    uint64_t                dir_code;

    easy_debug_log("http_status_read_dir: %s", cp.monitor_dir);

    // 读掉notify里的数据
    if (w && cp.ifd >= 0) {
        do {
            n = read(cp.ifd, buffer, HTTP_STATUS_MAX_BUFFER);
        } while(n == HTTP_STATUS_MAX_BUFFER);
    }

    // 读目录
    fd = open(cp.monitor_dir, O_RDONLY | O_NONBLOCK | O_DIRECTORY);
    cnt = syscall(SYS_getdents, fd, buffer, HTTP_STATUS_MAX_BUFFER);
    close(fd);

    // 比较hashcode
    dir_code = easy_hash_code(buffer, cnt, 5);

    if (cp.dir_cnt == cnt && cp.dir_code == dir_code)
        return;

    if (cp.dir_pool) easy_pool_destroy(cp.dir_pool);

    cp.dir_cnt = cnt;
    cp.dir_code = dir_code;
    cp.dir_pool = easy_pool_create(2048);
    n = offsetof(http_status_file_t, node);
    cp.dir_table = easy_hash_create(cp.dir_pool, 128, n);

    // 复制
    p = (char *)easy_pool_alloc(cp.dir_pool, cnt);
    memcpy(p, buffer, cnt);

    // 建立table
    n = 0;
    http_status_dirent_t    *dirp;
    http_status_file_t      *file;

    while(n < cnt) {
        dirp = (http_status_dirent_t *)(p + n);

        if (strcmp(dirp->d_name, ".") && strcmp(dirp->d_name, "..")) {
            file = (http_status_file_t *)easy_pool_alloc(cp.dir_pool, sizeof(http_status_file_t));
            file->name = dirp->d_name;
            easy_debug_log("found file: %s\n", file->name);
            dir_code = easy_hash_code(file->name, strlen(file->name), 5);
            easy_hash_add(cp.dir_table, dir_code, &file->node);
        }

        // next
        n += dirp->d_reclen;
    }
}

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static void http_status_read_vip(struct ev_loop *loop, ev_timer *w, int revents)
{
    int                     fd, n;
    struct ifconf           ifc;
    struct ifreq            *ifr;
    uint64_t                vip_code;
    easy_addr_t             ip;

    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
        return;

    ifc.ifc_len = sizeof(struct ifreq) * 64;
    char                    buffer[ifc.ifc_len];
    ifc.ifc_buf = buffer;

    if (ioctl(fd, SIOCGIFCONF, (char *)&ifc) < 0)
        goto error_exit;

    // 比较hashcode
    vip_code = easy_hash_code(buffer, ifc.ifc_len, 5);

    if (cp.vip_cnt == ifc.ifc_len && cp.vip_code == vip_code)
        goto error_exit;

    if (cp.vip_pool) easy_pool_destroy(cp.vip_pool);

    cp.vip_cnt = ifc.ifc_len;
    cp.vip_code = vip_code;
    cp.vip_pool = easy_pool_create(0);
    n = offsetof(http_status_vip_t, node);
    cp.vip_table = easy_hash_create(cp.vip_pool, 64, n);

    // 只从中lo:
    ifr = ifc.ifc_req;
    http_status_vip_t       *vip;

    for (n = 0; n < ifc.ifc_len; n += sizeof(struct ifreq), ifr++) {
        if (strncmp(ifr->ifr_name, "lo", 2))
            continue;

        memcpy(&ip, &(ifr->ifr_addr), sizeof(uint64_t));

        if (ioctl(fd, SIOCGIFNETMASK, ifr) || (~ * ((uint64_t *)&ifr->ifr_addr) >> 32))
            continue;

        vip = (http_status_vip_t *)easy_pool_alloc(cp.vip_pool, sizeof(http_status_vip_t));
        easy_debug_log("found vip: %s\n", easy_inet_addr_to_str(&ip, buffer, 32));
        vip->addr = ip.u.addr;
        easy_hash_add(cp.vip_table, vip->addr, &vip->node);
    }

error_exit:
    close(fd);
}

static void daemonize(const char *pidfile)
{
    int                     fd, pid;

    pid = fork();

    if (pid < 0) exit(1);

    if (pid > 0) exit(0);

    setsid();
    chdir("/");
    daemon(0, 0);

    if ((fd = open(pidfile, O_RDWR | O_CREAT, 0640)) >= 0) {
        char                    str[32];
        lnprintf(str, 32, "%d\n", getpid());
        write(fd, str, strlen(str));
        close(fd);
    }

}
