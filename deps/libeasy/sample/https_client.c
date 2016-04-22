#include <getopt.h>
#include <easy_io.h>
#include <easy_time.h>
#include "easy_http_handler.h"

// 命令行参数结构
typedef struct cmdline_param {
    easy_addr_t             address;
    easy_io_handler_pt      io_handler;
    int64_t                 request_cnt;
    int                     keep_alive;
    easy_buf_t              *raw_header;
    easy_string_pair_t      *headers;
    easy_pool_t             *pool;
    char                    ssl_file[256];
    char                    url[1024];
    char                    servername[128];

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
static int http_client_disconnect(easy_connection_t *c);
static int http_client_send_request(easy_addr_t addr);

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
    cp.request_cnt = 10;

    // parse cmd line
    cp.pool = easy_pool_create(8192);

    if (parse_cmd_line(argc, argv, &cp) == EASY_ERROR)
        return EASY_ERROR;

    // 检查必需参数
    if (cp.address.family == 0 || cp.ssl_file[0] == '\0') {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    easy_ssl_init();

    // 对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(1)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    if ((easy_io_var.ssl = easy_ssl_config_load(cp.ssl_file)) == NULL) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    // 为监听端口设置处理函数，并增加一个监听端口
    cp.io_handler.decode = easy_http_client_on_decode;
    cp.io_handler.encode = easy_http_client_on_encode;
    cp.io_handler.on_disconnect = http_client_disconnect;

    // 准备发送内容
    size = 128 + strlen(cp.url);
    header = cp.headers;

    while(header) {
        size += header->name.len;
        header = header->next;
    }

    b = easy_buf_create(cp.pool, size);
    b->last += lnprintf(b->last, (b->end - b->last), "GET %s HTTP/1.1\r\n", cp.url);
    header = cp.headers;

    while(header) {
        b->last += lnprintf(b->last, (b->end - b->last),
                            "%s\r\n", easy_buf_string_ptr(&header->name));
        header = header->next;
    }

    b->last += lnprintf(b->last, (b->end - b->last), "\r\n");
    cp.raw_header = b;

    // 创建连接
    easy_session_t          *s = easy_connection_connect_init(NULL, &cp.io_handler, 0, NULL, EASY_CONNECT_SSL, cp.servername);

    if (easy_connection_connect_ex(&easy_io_var, cp.address, s) != EASY_OK) {
        char                    buffer[32];
        easy_error_log("connection failure: %s\n", easy_inet_addr_to_str(&cp.address, buffer, 32));
    }

    if (easy_io_start()) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // send request
    int64_t                 t1 = easy_time_now();

    for(i = 0; i < cp.request_cnt; i++) {
        if (http_client_send_request(cp.address) != EASY_OK)
            break;
    }

    int64_t                 t2 = easy_time_now();
    double                  speed = cp.request_cnt * 1000000 / (t2 - t1);

    if (i == cp.request_cnt) easy_error_log("QPS: %.2f\n", speed);

    easy_io_stop();

    // 等待线程退出
    ret = easy_io_wait();
    easy_ssl_config_destroy(easy_io_var.ssl);
    easy_io_destroy();
    easy_ssl_cleanup();
    easy_pool_destroy(cp.pool);

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -S host:port -f ssl.conf [-n req_cnt] url\n"
            "    -S, --server            server address\n"
            "    -f, --ssl_file          ssl.conf\n"
            "    -n, --req_cnt           request count\n"
            "    -N, --name              server name\n"
            "    -k, --keep_alive        keep_alive\n"
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
    const char              *opt_string = "hVS:n:t:H:kf:N:";
    easy_string_pair_t      *header;
    char                    *url;
    struct option           long_opts[] = {
        {"server", 1, NULL, 'S'},
        {"header", 1, NULL, 'H'},
        {"ssl_file", 1, NULL, 'f'},
        {"req_cnt", 1, NULL, 'n'},
        {"name", 1, NULL, 'N'},
        {"keep_alive", 0, NULL, 'k'},
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

        case 'n':
            cp->request_cnt = atoi(optarg);
            break;

        case 'N':
            snprintf(cp->servername, 128, optarg);

        case 'k':
            cp->keep_alive = 1;
            break;

        case 'f':
            if (realpath(optarg, cp->ssl_file) == NULL) {
                cp->ssl_file[0] = '\0';
                fprintf(stderr, "filename: %s not found.\n", optarg);
                return EASY_ERROR;
            }

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

    char                    *purl = argv[optind];

    if (strncmp(purl, "https://", 8) != 0) {
        fprintf(stderr, "request url invalidate: %s\n", purl);
        return EASY_ERROR;
    }

    purl += 8;

    if ((url = strchr(purl, '/')) != NULL) {
        snprintf(cp->url, 1024, "%s", url);
    }

    // 处理url请求
    if (url) *url = '\0';

    int                     port = 443;

    if (cp->servername != NULL) {
        snprintf(cp->servername, 128, "%s", purl);
        char                    *p = strchr(cp->servername, ':');

        if (p) {
            *p ++ = '\0';
            port = atoi(p);
        }
    }

    if (cp->address.family == 0) {
        cp->address = easy_inet_str_to_addr(purl, port);
    }

    header = (easy_string_pair_t *) easy_pool_calloc(cp->pool, sizeof(easy_string_pair_t));
    header->next = cp->headers;
    easy_buf_string_printf(cp->pool, &header->name, "Host: %s", purl);
    cp->headers = header;

    if (url) *url = '/';

    return EASY_OK;
}

/**
 * 新建packet
 */
static int http_client_send_request(easy_addr_t addr)
{
    easy_session_t          *s;
    easy_http_packet_t      *packet;
    easy_http_request_t     *reply;
    easy_buf_t              *b;
    int                     ret = EASY_OK;

    if ((packet = easy_session_packet_create(easy_http_packet_t, s, 0)) == NULL)
        return EASY_ERROR;

    b = easy_buf_pack(s->pool, cp.raw_header->pos, cp.raw_header->last - cp.raw_header->pos);
    packet->is_raw_header = 1;
    easy_buf_chain_offer(&packet->output, b);

    easy_atomic_inc(&cp.send_cnt);
    easy_session_set_request(s, packet, 5000, NULL);
    reply = easy_io_send(addr, s);

    if (cp.request_cnt <= 10)
        easy_error_log("reply: %p, length: %d", reply, (reply ? reply->str_body.len : 0));

    if (reply == NULL) {
        easy_error_log("reply is null.");
        ret = EASY_ERROR;
    }

    easy_session_destroy(s);
    return ret;
}

static int http_client_disconnect(easy_connection_t *c)
{
    if (c->conn_has_error) {
        easy_info_log("connection disconnect.\n");
        easy_io_stop();
    }

    return EASY_OK;
}
