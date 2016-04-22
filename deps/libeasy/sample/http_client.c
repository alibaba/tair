#include <getopt.h>
#include <easy_io.h>
#include "easy_http_handler.h"

// 命令行参数结构
typedef struct cmdline_param {
    easy_addr_t             address;
    easy_io_handler_pt      io_handler;
    int                     io_thread_cnt;
    int                     connect_cnt;
    int                     islarge, file_len;
    int64_t                 request_cnt;
    int                     keep_alive;
    easy_buf_t              *raw_header;
    easy_string_pair_t      *headers;
    easy_pool_t             *pool;
    char                    url[256];

    // count
    ev_tstamp               start_time;
    easy_atomic_t           send_cnt;
    easy_atomic_t           process_cnt;
    easy_atomic_t           error_cnt;
    easy_atomic_t           timeout_cnt;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int http_client_on_process(easy_request_t *r);
static int http_client_new_packet(easy_connection_t *c);
static int http_client_disconnect(easy_connection_t *c);
static int http_client_set_data(easy_request_t *r, const char *data, int len);
static void print_speed();

/**
 * 程序入口
 */
int main(int argc, char **argv)
{
    int                     i, ret, size;
    easy_string_pair_t      *header;
    easy_buf_t              *b;

    // default
    memset(&cp, 0, sizeof(cmdline_param));
    cp.io_thread_cnt = 1;
    cp.connect_cnt = 1;
    cp.request_cnt = 1000;

    // parse cmd line
    cp.pool = easy_pool_create(8192);

    if (parse_cmd_line(argc, argv, &cp) == EASY_ERROR)
        return EASY_ERROR;

    // 检查必需参数
    if (cp.address.family == 0) {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    // 对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(cp.io_thread_cnt)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    // 为监听端口设置处理函数，并增加一个监听端口
    cp.io_handler.decode = easy_http_client_on_decode;
    cp.io_handler.encode = easy_http_client_on_encode;
    cp.io_handler.process = http_client_on_process;
    cp.io_handler.new_packet = http_client_new_packet;
    cp.io_handler.on_disconnect = http_client_disconnect;

    if (cp.islarge) cp.io_handler.set_data = http_client_set_data;

    // 准备发送内容
    size = 128 + strlen(cp.url);
    header = cp.headers;

    while(header) {
        size += header->name.len;
        header = header->next;
    }

    b = easy_buf_create(cp.pool, size);
    b->last += lnprintf(b->last, (b->end - b->last), "GET %s HTTP/1.1\r\nConnection: keep-alive\r\n", cp.url);
    header = cp.headers;

    while(header) {
        b->last += lnprintf(b->last, (b->end - b->last),
                            "%s\r\n", easy_buf_string_ptr(&header->name));
        header = header->next;
    }

    b->last += lnprintf(b->last, (b->end - b->last), "\r\n");
    cp.raw_header = b;

    // 创建连接
    easy_addr_t             addr = cp.address;

    for(i = 0; i < cp.connect_cnt; i++) {
        addr.cidx = i;

        if (easy_io_connect(addr, &cp.io_handler, 0, NULL) != EASY_OK) {
            char                    buffer[32];
            easy_error_log("connection failure: %s\n", easy_inet_addr_to_str(&cp.address, buffer, 32));
            break;
        }
    }

    // 起线程并开始
    cp.start_time = ev_time();

    // 起处理速度统计定时器
    ev_timer                stat_watcher;
    easy_io_stat_t          iostat;
    easy_io_stat_watcher_start(&stat_watcher, 5.0, &iostat, NULL);

    if (easy_io_start()) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // 等待线程退出
    ret = easy_io_wait();

    // speed
    print_speed();
    easy_io_destroy();
    easy_pool_destroy(cp.pool);

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -S host:port [-c conn_cnt] [-n req_cnt] [-t thread_cnt] url\n"
            "    -S, --server            server address\n"
            "    -c, --conn_cnt          connection count\n"
            "    -n, --req_cnt           request count\n"
            "    -L, --large_file        large file\n"
            "    -k, --keep_alive        keep_alive\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
            "    -H, --header            add custom header\n"
            "    -h, --help              display this help and exit\n"
            "    -V, --version           version and build time\n\n"
            "eg: %s http://localhost/a.html\n\n", prog_name, prog_name);
}

/**
 * 解析命令行
 */
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp)
{
    int                     opt;
    const char              *opt_string = "hVS:c:n:t:H:kL";
    easy_string_pair_t      *header;
    char                    *url;
    struct option           long_opts[] = {
        {"server", 1, NULL, 'S'},
        {"header", 1, NULL, 'H'},
        {"conn_cnt", 1, NULL, 'c'},
        {"req_cnt", 1, NULL, 'n'},
        {"io_thread_cnt", 1, NULL, 't'},
        {"keep_alive", 0, NULL, 'k'},
        {"large_file", 0, NULL, 'L'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {0, 0, 0, 0}
    };

    opterr = 0;

    while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
        switch (opt) {
        case 'S':
            cp->address = easy_inet_str_to_addr(optarg, 0);
            break;

        case 't':
            cp->io_thread_cnt = atoi(optarg);
            break;

        case 'c':
            cp->connect_cnt = atoi(optarg);
            break;

        case 'n':
            cp->request_cnt = atoi(optarg);
            break;

        case 'k':
            cp->keep_alive = 1;
            break;

        case 'L':
            cp->islarge = 1;
            break;

        case 'H':
            header = (easy_string_pair_t *) easy_pool_calloc(cp->pool, sizeof(easy_string_pair_t));
            header->next = cp->headers;
            easy_buf_string_set(&header->name, optarg);
            cp->headers = header;
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

    if (optind != argc - 1) {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    if (cp->islarge) {
        cp->connect_cnt = 1;
        cp->request_cnt = 1;
    }

    if (strncmp(argv[optind], "http://", 7) != 0) {
        fprintf(stderr, "request url invalidate: %s\n", argv[optind]);
        return EASY_ERROR;
    }

    if ((url = strchr(argv[optind] + 7, '/')) != NULL) {
        strncpy(cp->url, url, 256);
        cp->url[255] = '\0';
    }

    // 处理url请求
    if (url) *url = '\0';

    if (cp->address.family == 0) {
        cp->address = easy_inet_str_to_addr(argv[optind] + 7, 80);
    }

    header = (easy_string_pair_t *) easy_pool_calloc(cp->pool, sizeof(easy_string_pair_t));
    header->next = cp->headers;
    easy_buf_string_printf(cp->pool, &header->name, "Host: %s", argv[optind] + 7);
    cp->headers = header;

    if (url) *url = '/';

    return EASY_OK;
}

static int http_client_set_data(easy_request_t *r, const char *data, int len)
{
    write(1, data, len);
    cp.file_len += len;
    return EASY_OK;
}

/**
 * 处理函数
 */
static int http_client_on_process(easy_request_t *r)
{
    easy_http_request_t     *reply;

    reply = (easy_http_request_t *) r->ipacket;

    easy_atomic_inc(&cp.process_cnt);

    if (reply) {
        if (reply->parser.status_code != 200) {
            easy_atomic_inc(&cp.error_cnt);
        }
    } else {
        easy_atomic_inc(&cp.timeout_cnt);
    }

    if (cp.send_cnt >= cp.request_cnt && cp.process_cnt == cp.send_cnt) {
        easy_io_stop();
    }

    //if (cp.keep_alive == 0) r->ms->c->wait_close = 1;

    easy_session_destroy(r->ms);

    return (reply ? EASY_OK : EASY_ERROR);
}

/**
 * 新建packet
 */
static int http_client_new_packet(easy_connection_t *c)
{
    easy_session_t          *s;
    easy_http_packet_t      *packet;
    easy_buf_t              *b;

    if (cp.send_cnt >= cp.request_cnt)
        return EASY_OK;

    if (cp.keep_alive == 0 && c->con_summary->doing_request_count + c->con_summary->done_request_count > 0)
        return EASY_OK;

    if ((packet = easy_session_packet_create(easy_http_packet_t, s, 0)) == NULL)
        return EASY_ERROR;

    b = easy_buf_pack(s->pool, cp.raw_header->pos, cp.raw_header->last - cp.raw_header->pos);
    packet->is_raw_header = 1;
    easy_buf_chain_offer(&packet->output, b);

    easy_atomic_inc(&cp.send_cnt);

    easy_session_set_request(s, packet, 5000, NULL);
    return easy_connection_send_session(c, s);
}

static int http_client_disconnect(easy_connection_t *c)
{
    easy_io_stop();
    return EASY_OK;
}

static void print_speed()
{
    double                  t = ev_time() - cp.start_time;
    // speed
    easy_error_log("process_cnt: %" PRIdFAST32 "\nsend_cnt: %" PRIdFAST32
                   ", error_cnt: %" PRIdFAST32 ", timeout_cnt: %" PRIdFAST32 "\n",
                   cp.process_cnt, cp.send_cnt, cp.error_cnt, cp.timeout_cnt);
    easy_error_log("QPS: %.2f\n", 1.0 * cp.process_cnt / t);

    if (cp.islarge) easy_error_log("cp.file_len=%d", cp.file_len);
}
