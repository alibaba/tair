#include <getopt.h>
#include <easy_io.h>
#include "echo_packet.h"

// 命令行参数结构
typedef struct cmdline_param {
    int                     port;
    int                     io_thread_cnt;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int echo_process(easy_request_t *r);
static int echo_connect(easy_connection_t *c);

/**
 * 程序入口
 */
int main(int argc, char **argv)
{
    cmdline_param           cp;
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
    easy_io_var.tcp_defer_accept = 0;
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = echo_decode;
    io_handler.encode = echo_encode;
    io_handler.process = echo_process;
    io_handler.on_connect = echo_connect;

    if ((listen = easy_io_add_listen(NULL, cp.port, &io_handler)) == NULL) {
        easy_error_log("easy_io_add_listen error, port: %d, %s\n",
                       cp.port, strerror(errno));
        return EASY_ERROR;
    } else {
        easy_error_log("listen start, port = %d\n", cp.port);
    }

    // 起处理速度统计定时器
    ev_timer                stat_watcher;
    easy_io_stat_t          iostat;
    easy_io_stat_watcher_start(&stat_watcher, 5.0, &iostat, NULL);

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
    fprintf(stderr, "%s -p port [-t thread_cnt]\n"
            "    -p, --port              server port\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
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
    const char              *opt_string = "hVp:t:";
    struct option           long_opts[] = {
        {"port", 1, NULL, 'p'},
        {"io_thread_cnt", 1, NULL, 't'},
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

        case 't':
            cp->io_thread_cnt = atoi(optarg);
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
static int echo_process(easy_request_t *r)
{
    // 直接用in packet
    r->opacket = r->ipacket;
    return EASY_OK;
}

static int echo_connect(easy_connection_t *c)
{
    char                    *str = "hello, world!\n";
    write(c->fd, str, strlen(str) + 1);
    return EASY_OK;
}

