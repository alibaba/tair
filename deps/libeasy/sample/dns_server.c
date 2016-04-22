#include <getopt.h>
#include <easy_io.h>
#include <easy_http_handler.h>
#include <fcntl.h>

// 命令行参数结构
typedef struct cmdline_param {
    int                     port;
    int                     io_thread_cnt;
    int                     print_stat;
    char                    root_dir[256];
    easy_thread_pool_t      *threads;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int easy_dns_server_on_process(easy_request_t *r);
static int easy_dns_server_on_encode(easy_request_t *r, void *data);
static void *easy_dns_server_on_decode(easy_message_t *m);

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
    cp.io_thread_cnt = 1;

    // parse cmd line
    if (parse_cmd_line(argc, argv, &cp) == EASY_ERROR)
        return EASY_ERROR;

    // 检查必需参数
    if (cp.port == 0) {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    // 对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(cp.io_thread_cnt)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    // 为监听端口设置处理函数，并增加一个监听端口
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = easy_dns_server_on_decode;
    io_handler.encode = easy_dns_server_on_encode;
    io_handler.process = easy_dns_server_on_process;
    io_handler.is_udp = 1;

    if ((listen = easy_io_add_listen(NULL, cp.port, &io_handler)) == NULL) {
        easy_error_log("easy_io_add_listen error, port: %d, %s\n",
                       cp.port, strerror(errno));
        return EASY_ERROR;
    } else {
        easy_error_log("listen start, port = %d\n", cp.port);
    }

    // 起处理速度统计定时器
    if (cp.print_stat) {
        ev_timer                stat_watcher;
        easy_io_stat_t          iostat;
        easy_io_stat_watcher_start(&stat_watcher, 5.0, &iostat, NULL);
    }

    // 起线程并开始
    if (easy_io_start()) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // 等待线程退出
    ret = easy_io_wait();
    easy_io_destroy();

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -p port [-R root_dir] [-t thread_cnt]\n"
            "    -p, --port              server port\n"
            "    -R, --root_dir          root directory\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
            "    -D, --file_thread_cnt   thread count for disk io, default: 1\n"
            "    -s, --print_stat        print statistics\n"
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
    const char              *opt_string = "hVp:t:R:D:s";
    struct option           long_opts[] = {
        {"port", 1, NULL, 'p'},
        {"root_dir", 1, NULL, 'R'},
        {"io_thread_cnt", 1, NULL, 't'},
        {"file_thread_cnt", 1, NULL, 'D'},
        {"print_stat", 0, NULL, 's'},
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

        case 'R':

            if (realpath(optarg, cp->root_dir) == NULL) {
                cp->root_dir[0] = '\0';
                fprintf(stderr, "directory: %s not found.\n", optarg);
                return EASY_ERROR;
            }

            break;

        case 't':
            cp->io_thread_cnt = atoi(optarg);
            break;

        case 's':
            cp->print_stat = 1;
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

static int easy_dns_server_on_encode(easy_request_t *r, void *data)
{
    easy_buf_t              *b = easy_buf_create(r->ms->pool, 80);
    static char             text[] = {0x0, 0x8, 0x85, 0x80, 0x0, 0x1, 0x0, 0x1, 0x0, 0x1, 0x0, 0x1, 0x3, 0x77, 0x77, 0x77, 0x4, 0x74, 0x65, 0x73, 0x74, 0x3, 0x63, 0x6f, 0x6d, 0x0, 0x0, 0x1, 0x0, 0x1, 0xc0,
                                      0xc, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x96, 0x0, 0x0, 0x4, 0xa, 0xe8, 0x4, 0x1f, 0xc0, 0x10, 0x0, 0x2, 0x0, 0x1, 0x0, 0x0, 0x96, 0x0, 0x0, 0x6, 0x3, 0x6e, 0x73, 0x31, 0xc0, 0x10,
                                      0xc0, 0x3a, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x96, 0x0, 0x0, 0x4, 0xc0, 0xa8, 0x0, 0x1
                                     };
    memcpy(b->last, text, 80);
    int                     x = (long)data;
    x --;
    *((int16_t *)b->last) = (x & 0xffff);
    b->last += 80;
    easy_request_addbuf(r, b);
    return EASY_OK;
}
static void *easy_dns_server_on_decode(easy_message_t *m)
{
    int                     x = *((int *)m->input->pos);
    x ++;
    m->input->pos = m->input->last;
    return (void *)(long)x;
}
/**
 * 处理函数
 */
static int easy_dns_server_on_process(easy_request_t *r)
{
    r->opacket = r->ipacket;
    //r->ms->c->wait_close = 1;
    return EASY_OK;
}

