#include <getopt.h>
#include <easy_io.h>
#include <easy_http_handler.h>
#include <fcntl.h>

// 命令行参数结构
typedef struct cmdline_param {
    int                     port;
    int                     io_thread_cnt;
    int                     print_stat;
    uint64_t                address;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int easy_http_server_on_process(easy_request_t *r);

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
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = easy_http_server_on_process;

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
    fprintf(stderr, "%s -p port [-t thread_cnt] -H xxx.xxx.xxx.xxx:8000\n"
            "    -p, --port              server port\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
            "    -H, --host              destination server host\n"
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
    const char              *opt_string = "hVp:t:H:s";
    struct option           long_opts[] = {
        {"port", 1, NULL, 'p'},
        {"io_thread_cnt", 1, NULL, 't'},
        {"host", 1, NULL, 'H'},
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

        case 't':
            cp->io_thread_cnt = atoi(optarg);
            break;

        case 'H':
            cp->address = easy_inet_str_to_addr(optarg, 0);

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

    if (cp->address == 0) {
        easy_warn_log("host address error!\n");
        print_usage(argv[0]);
        exit(0);
    }

    return EASY_OK;
}

/**
 * 新建packet
 */
int http_fetch_new_packet(easy_connection_t *fc)
{
    easy_session_t          *s;
    easy_http_packet_t      *packet;
    easy_buf_t              *b;

    if (fc->doing_request_count + fc->done_request_count > 0)
        return EASY_OK;

    if ((packet = easy_session_packet_create(easy_http_packet_t, s, 0)) == NULL)
        return EASY_ERROR;

    easy_request_t          *cr = (easy_request_t *)fc->handler->user_data;

    if (cr == NULL)
        return EASY_ERROR;

    easy_http_request_t     *cp = (easy_http_request_t *)cr->ipacket;

    b = easy_buf_create(s->pool, 512);
    b->last += lnprintf(b->last, (b->end - b->last), "GET %s HTTP/1.1\r\n", easy_buf_string_ptr(&cp->str_path));
    b->last += lnprintf(b->last, (b->end - b->last), "User-Agent: http_server_proxy\r\n");
    b->last += lnprintf(b->last, (b->end - b->last), "Accept: */*\r\n");
    b->last += lnprintf(b->last, (b->end - b->last), "Host: xxx.xxx.xxx.xxx:8000\r\n");
    b->last += lnprintf(b->last, (b->end - b->last), "\r\n");

    packet->is_raw_header = 1;
    easy_buf_chain_offer(&packet->output, b);

    easy_session_set_request(s, packet, 5000, NULL);

    int                     ret = 0;

    if ((ret = easy_session_dispatch(fc, s)) == EASY_ERROR)
        easy_warn_log("easy_connection_send_session == EASY_ERROR\n");

    return ret;
}

/**
 * 处理函数
 */
static int http_fetch_on_process(easy_request_t *fr)
{
    easy_http_request_t     *reply;

    easy_request_t          *cr = (easy_request_t *)fr->ms->c->handler->user_data;

    if (cr == NULL) {
        easy_session_destroy(fr->ms);
        easy_warn_log("http_fetch_on_process: handler->user_data == NULL\n");
        return EASY_ERROR;
    }

    fr->ms->c->wait_close = 1;
    cr->args = NULL;

    reply = (easy_http_request_t *) fr->ipacket;

    if (reply == NULL) {
        easy_session_destroy(fr->ms);
        easy_warn_log("http_fetch_on_process: reply == NULL\n");
        return EASY_ERROR;
    }

    easy_buf_t              *b = easy_buf_pack(cr->ms->pool, reply->str_body.data, reply->str_body.len);
    easy_request_addbuf(cr, b);
    easy_session_destroy(fr->ms);

    return easy_request_do_reply(cr) == EASY_OK ? EASY_OK : EASY_ERROR;
}

int http_fetch_disconnect(easy_connection_t *fc)
{
    easy_connection_t       *cc = (easy_connection_t *)fc->user_data;
    easy_atomic_dec(&cc->ref);

    if (cc->status == EASY_CONN_CLOSE && cc->ref == 0)
        easy_connection_destroy(cc);

    return EASY_OK;
}

/**
 * 处理函数
 */
static int easy_http_server_on_process(easy_request_t *r)
{
    easy_http_request_t         *p;

    r->opacket = p = (easy_http_request_t *)r->ipacket;
    easy_http_header_string_end(p);

    // 处理文件名
    if (p->str_path.len == 0) {
        fprintf(stderr, "p->str_path.len: %d\n", p->str_path.len);
        return EASY_ERROR;
    }

    // 建立与源服务器的连接，去源服务器取数据，每个请求新建一个到源服务器的连接
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)easy_pool_calloc(r->ms->c->pool, sizeof(easy_io_handler_pt));
    memset(io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler->decode = easy_http_client_on_decode;
    io_handler->encode = easy_http_client_on_encode;
    io_handler->process = http_fetch_on_process;
    io_handler->new_packet = http_fetch_new_packet;
    io_handler->on_disconnect = http_fetch_disconnect;
    io_handler->user_data = r;
    easy_connection_t       *fc;

    if ((fc = easy_io_connect_addr(cp.address, io_handler, 0, NULL)) == NULL) {
        char                    buffer[32];
        easy_error_log("connection failure: %s\n", easy_inet_addr_to_str(&cp.address, buffer, 32));
        return EASY_ERROR;
    }

    // 用于方便客户端的请求获取这个请求到源服务器的链接信息
    r->args = fc;
    // 释放掉fc的时候，判断request connection是否需要被释放。
    fc->user_data = r->ms->c;
    // 锁住request connection在fetch connection处理完之前不被释放
    easy_atomic_inc(&r->ms->c->ref);

    return EASY_AGAIN;
}

