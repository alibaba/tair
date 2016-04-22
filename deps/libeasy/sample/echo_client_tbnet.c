#include <getopt.h>
#include <easy_io.h>
#include "echo_packet_tbnet.h"

// 命令行参数结构
typedef struct cmdline_param {
    easy_addr_t             address;
    int                     work_thread_cnt;
    int                     io_thread_cnt;
    int                     connect_cnt;
    int64_t                 request_cnt;
    int                     request_size;
    char                    *send_data;

    // count
    easy_atomic_t           chid;
    int64_t                 request_byte;
    easy_atomic_t           process_byte;
    easy_atomic_t           send_byte;
    ev_tstamp               start_time;
    easy_atomic_t           connected;
    easy_addr_t             *conns_addr;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int echo_tbnet_process(easy_request_t *r);
static int echo_tbnet_new_packet(easy_connection_t *c);
static int echo_tbnet_heartbeat(easy_connection_t *c);
static int echo_tbnet_disconnect(easy_connection_t *c);
static int echo_tbnet_disconnect2(easy_connection_t *c);
static int echo_tbnet_connect(easy_connection_t *c);
static int echo_tbnet_connect2(easy_connection_t *c);
static void print_speed();
static int connect_server(int idx, easy_io_handler_pt *io_handler);
static void dosend();

/**
 * 程序入口
 */
int main(int argc, char **argv)
{
    int                     i, ret;
    easy_io_handler_pt      io_handler;

    // default
    memset(&cp, 0, sizeof(cmdline_param));
    cp.io_thread_cnt = 1;
    cp.connect_cnt = 1;
    cp.request_cnt = 1000;
    cp.request_size = 512;

    // parse cmd line
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
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = echo_tbnet_decode;
    io_handler.encode = echo_tbnet_encode;
    io_handler.get_packet_id = echo_tbnet_packet_id;

    if (cp.work_thread_cnt == 0) {
        io_handler.new_packet = echo_tbnet_new_packet;
        io_handler.on_disconnect = echo_tbnet_disconnect;
        io_handler.on_connect = echo_tbnet_connect;
        io_handler.process = echo_tbnet_process;
    } else {
        io_handler.on_connect = echo_tbnet_connect2;
        io_handler.on_disconnect = echo_tbnet_disconnect2;
        io_handler.process = easy_client_wait_process;
        io_handler.batch_process = easy_client_wait_batch_process;
        io_handler.on_idle = echo_tbnet_heartbeat;
    }

    // 起线程并开始
    cp.start_time = ev_time();
    cp.request_byte = cp.request_cnt * cp.request_size;
    cp.send_data = (char *)easy_pool_alloc(easy_io_var.pool, cp.request_size);
    memset(cp.send_data, 'A', cp.request_size);

    if (cp.request_byte < 0) cp.request_byte = ~(((int64_t)1) << 63);

    // 起处理速度统计定时器
    ev_timer                stat_watcher;
    easy_io_stat_t          iostat;
    easy_io_stat_watcher_start(&stat_watcher, 5.0, &iostat, NULL);

    if (easy_io_start()) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // 创建连接
    cp.conns_addr = (easy_addr_t *)easy_pool_alloc(easy_io_var.pool,
                    sizeof(easy_addr_t) * cp.connect_cnt);

    for(i = 0; i < cp.connect_cnt; i++) {
        if (connect_server(i, &io_handler))
            break;

        if (i - cp.connected > 100) usleep(1);
    }

    if (easy_io_var.stoped == 0 && cp.work_thread_cnt > 0) {
        dosend();
    }

    // 等待线程退出
    ret = easy_io_wait();

    // speed
    print_speed();
    easy_io_destroy();

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -H host:port -c conn_cnt -n req_cnt -s size [-t thread_cnt]\n"
            "    -H, --host              server address\n"
            "    -c, --conn_cnt          connection count\n"
            "    -n, --req_cnt           request count\n"
            "    -s, --req_size          packet size of every request\n"
            "    -w, --work_thread_cnt   thread count for send, default: 0\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
            "    -h, --help              display this help and exit\n"
            "    -V, --version           version and build time\n\n"
            "eg: %s -Hlocalhost:5000 -c2 -n1000 -s512\n\n", prog_name, prog_name);
}

/**
 * 解析命令行
 */
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp)
{
    int                     opt;
    const char              *opt_string = "hVH:c:n:s:t:w:";
    struct option           long_opts[] = {
        {"host", 1, NULL, 'H'},
        {"conn_cnt", 1, NULL, 'c'},
        {"req_cnt", 1, NULL, 'n'},
        {"req_size", 1, NULL, 's'},
        {"work_thread_cnt", 1, NULL, 'w'},
        {"io_thread_cnt", 1, NULL, 't'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {0, 0, 0, 0}
    };

    opterr = 0;

    while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
        switch (opt) {
        case 'H':
            cp->address = easy_inet_str_to_addr(optarg, 0);
            break;

        case 'w':
            cp->work_thread_cnt = atoi(optarg);
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

        case 's':

            if ((cp->request_size = atoi(optarg)) < 128)
                cp->request_size = 128;

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
static int echo_tbnet_process(easy_request_t *r)
{
    echo_packet_tbnet_t           *reply;

    reply = (echo_packet_tbnet_t *) r->ipacket;
    easy_session_destroy(r->ms);

    if (!reply)
        return EASY_ERROR;

    easy_atomic_add(&cp.process_byte, reply->datalen);

    if (cp.send_byte >= cp.request_byte && cp.process_byte == cp.send_byte) {
        easy_warn_log("echo_tbnet_process disconnect.\n");
        easy_io_stop();
    }

    return EASY_OK;
}

/**
 * 新建packet
 */
static int echo_tbnet_new_packet(easy_connection_t *c)
{
    easy_session_t          *s;
    echo_packet_tbnet_t     *packet;

    if (cp.send_byte >= cp.request_byte)
        return EASY_OK;

    if ((packet = echo_packet_tbnet_new(&s, 0)) == NULL)
        return EASY_ERROR;

    easy_session_set_timeout(s, 5000);
    packet->data = cp.send_data;
    packet->datalen = cp.request_size;
    packet->chid = easy_atomic_add_return(&cp.chid, 1);
    easy_atomic_add(&cp.send_byte, cp.request_size);

    easy_connection_send_session(c, s);
    return EASY_OK;
}

static int echo_tbnet_heartbeat(easy_connection_t *c)
{
    return EASY_OK;
}

static int echo_tbnet_disconnect(easy_connection_t *c)
{
    easy_atomic_dec(&cp.connected);
    static int              cnt = 0;

    if (c->conn_has_error) {
        easy_warn_log("connect disconnect.\n");
        easy_io_stop();
    } else {
        easy_info_log("%d echo_tbnet_disconnect\n", ++cnt);
    }

    return EASY_OK;
}

static int echo_tbnet_connect2(easy_connection_t *c)
{
    c->idle_time = 10000;
    return EASY_OK;
}

static int echo_tbnet_disconnect2(easy_connection_t *c)
{
    if (c->conn_has_error) {
        easy_warn_log("connect disconnect.\n");
        //    easy_io_stop();
    } else if (easy_io_var.stoped) {
        //  easy_io_stop();
    }

    return EASY_OK;
}

static int echo_tbnet_connect(easy_connection_t *c)
{
    easy_atomic_inc(&cp.connected);
    return EASY_OK;
}

static void print_speed()
{
    double                  t = ev_time() - cp.start_time;
    // speed
    easy_error_log("process_byte: %" PRIdFAST32 ", send_byte: %" PRIdFAST32 "\n", cp.process_byte, cp.send_byte);
    easy_error_log("QPS: %.2f\n", 1.0 * cp.process_byte / t / cp.request_size);
    easy_error_log("Throughput: %.2f MB/s\n", 1.0 * cp.process_byte / t / 1024 / 1024);
}

static void *send_packet(void *args)
{
    echo_packet_tbnet_t     *packet, *resp;
    easy_session_t          *s;
    int                     idx = (long)args;
    easy_addr_t             addr = cp.conns_addr[idx % cp.connect_cnt];

    int64_t                 byte = cp.request_byte / cp.work_thread_cnt;
    int64_t                 sendbyte = 0;

    while(byte > 0 && easy_io_var.stoped == 0) {
        if ((packet = echo_packet_tbnet_new(&s, cp.request_size)) == NULL)
            break;

        easy_session_set_timeout(s, 2000);
        memcpy(packet->data, cp.send_data, cp.request_size);
        packet->data = cp.send_data;
        packet->datalen = cp.request_size;
        packet->chid = easy_atomic_add_return(&cp.chid, 1);
        sendbyte += cp.request_size;

        resp = (echo_packet_tbnet_t *)easy_io_send(addr, s);

        if (resp != NULL) {
            byte -= cp.request_size;
            easy_atomic_add(&cp.process_byte, resp->datalen);
        } else if (s->error) {
            break;
        }

        easy_session_destroy(s);
    }

    easy_atomic_add(&cp.send_byte, sendbyte);
    return (void *) NULL;
}

static void dosend()
{
    pthread_t               tids[cp.work_thread_cnt];
    int                     i;

    for(i = 0; i < cp.work_thread_cnt; i++) {
        pthread_create(&tids[i], NULL, send_packet, (void *)(long)i);
    }

    for(i = 0; i < cp.work_thread_cnt; i++) {
        pthread_join(tids[i], NULL);
    }

    easy_io_stop();
}

static int connect_server(int idx, easy_io_handler_pt *io_handler)
{
    easy_addr_t             addr = cp.address;
    addr.cidx = idx;

    if (easy_connection_connect(&easy_io_var, addr, io_handler, 0, NULL, 1) != EASY_OK) {
        char                    buffer[32];
        easy_error_log("connection failure: %s\n", easy_inet_addr_to_str(&cp.address, buffer, 32));
        return EASY_ERROR;
    }

    cp.conns_addr[idx] = addr;
    return EASY_OK;
}
