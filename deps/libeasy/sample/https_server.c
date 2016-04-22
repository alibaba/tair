#include <getopt.h>
#include <easy_io.h>
#include <easy_http_handler.h>
#include <fcntl.h>

// 命令行参数结构
typedef struct cmdline_param {
    int                     port;
    int                     io_thread_cnt;
    int                     print_stat;
    char                    ssl_file[256];
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int easy_http_server_on_process(easy_request_t *r);
static void easy_buf_string_tobuf(easy_pool_t *pool, easy_list_t *bc, char *name, easy_buf_string_t *s);
static void easy_http_request_tobuf(easy_pool_t *pool, easy_list_t *bc, easy_http_request_t *p);

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
    if (cp.port == 0 || cp.ssl_file[0] == '\0') {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    easy_ssl_init();

    // 对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(cp.io_thread_cnt)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    if ((easy_io_var.ssl = easy_ssl_config_load(cp.ssl_file)) == NULL) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    // 为监听端口设置处理函数，并增加一个监听端口
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = easy_http_server_on_process;
    io_handler.is_ssl = 1;

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
    easy_ssl_config_destroy(easy_io_var.ssl);
    easy_io_destroy();
    easy_ssl_cleanup();

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -p port -f ssl.conf [-t thread_cnt]\n"
            "    -p, --port              server port\n"
            "    -f, --ssl_file          ssl.conf\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
            "    -s, --print_stat        print statistics\n"
            "    -h, --help              display this help and exit\n"
            "    -V, --version           version and build time\n\n"
            "eg: %s -p 5000 -f ssl.conf\n\n", prog_name, prog_name);
}

/**
 * 解析命令行
 */
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp)
{
    int                     opt;
    const char              *opt_string = "hVp:t:f:s";
    struct option           long_opts[] = {
        {"port", 1, NULL, 'p'},
        {"ssl_file", 1, NULL, 'f'},
        {"io_thread_cnt", 1, NULL, 't'},
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

        case 'f':

            if (realpath(optarg, cp->ssl_file) == NULL) {
                cp->ssl_file[0] = '\0';
                fprintf(stderr, "filename: %s not found.\n", optarg);
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

/**
 * 处理函数
 */
static int easy_http_server_on_process(easy_request_t *r)
{
    easy_http_request_t         *p;

    p = (easy_http_request_t *)r->ipacket;
    easy_error_log("path: %s", (p->str_path.len ? p->str_path.data : ""));
    easy_http_request_tobuf(r->ms->pool, &p->output, p);
    r->opacket = p;

    return EASY_OK;
}

static void easy_buf_string_tobuf(easy_pool_t *pool, easy_list_t *bc, char *name, easy_buf_string_t *s)
{
    if (s->len) {
        easy_buf_t              *b = easy_buf_check_write_space(pool, bc, s->len + strlen(name) + 16);
        b->last += sprintf(b->last, "%s%.*s<br>\n", name, s->len, easy_buf_string_ptr(s));
    }
}

static void easy_http_request_tobuf(easy_pool_t *pool, easy_list_t *bc, easy_http_request_t *p)
{
    easy_string_pair_t      *t;

    easy_buf_string_tobuf(pool, bc, "path: ", &p->str_path);
    easy_buf_string_tobuf(pool, bc, "query_sring: ", &p->str_query_string);
    easy_buf_string_tobuf(pool, bc, "str_fragment: ", &p->str_fragment);
    easy_buf_string_tobuf(pool, bc, "str_body: ", &p->str_body);
    // headers
    easy_list_for_each_entry(t, &p->headers_in->list, list) {
        if (t->name.len && t->value.len) {
            easy_buf_t              *b = easy_buf_check_write_space(pool, bc, t->name.len + t->value.len + 16);
            b->last += sprintf(b->last, "%.*s: %.*s<br>\n",
                               t->name.len, easy_buf_string_ptr(&t->name),
                               t->value.len, easy_buf_string_ptr(&t->value));
        }
    }
}
