#include <getopt.h>
#include <easy_io.h>
#include <easy_time.h>
#include <easy_simple_handler.h>
#include <easy_http_handler.h>

// 命令行参数结构
typedef struct cmdline_param {
    int                     port;
    int                     io_thread_cnt;
    easy_io_t               *pio;
    easy_io_t               *cio;
    easy_io_t               *sio;
    easy_io_handler_pt      phandler;
    easy_io_handler_pt      chandler;
    easy_io_handler_pt      shandler;
    easy_addr_t             addr;
    easy_atomic32_t         chid;
} cmdline_param;

static cmdline_param cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);

static int proxy_server_on_process(easy_request_t *r);
static int proxy_client_on_process(easy_request_t *r);
static int server_on_process(easy_request_t *r);
static int server_on_simple_encode(easy_request_t *r, void *data);
static void *client_on_simple_decode(easy_message_t *m);


/**
 * 程序入口
 */
int main(int argc, char **argv)
{
    // default
    memset(&cp, 0, sizeof(cmdline_param));
    cp.io_thread_cnt = 1;
    cp.chid = 0x12345678;

    // parse cmd line
    if (parse_cmd_line(argc, argv, &cp) == EASY_ERROR)
        return EASY_ERROR;

    // 检查必需参数
    if (cp.port == 0) {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    // create
    cp.pio = easy_eio_create(NULL, cp.io_thread_cnt);
    cp.cio = easy_eio_create(NULL, cp.io_thread_cnt);
    cp.sio = easy_eio_create(NULL, cp.io_thread_cnt);

    if (cp.pio == NULL || cp.cio == NULL || cp.sio == NULL) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    cp.phandler.decode = easy_http_server_on_decode;
    cp.phandler.encode = easy_http_server_on_encode;
    cp.phandler.process = proxy_server_on_process;
    cp.chandler.decode = client_on_simple_decode;
    cp.chandler.encode = easy_simple_encode;
    cp.chandler.get_packet_id = easy_simple_packet_id;
    cp.chandler.process = proxy_client_on_process;
    cp.shandler.decode = easy_simple_decode;
    cp.shandler.encode = server_on_simple_encode;
    cp.shandler.get_packet_id = easy_simple_packet_id;
    cp.shandler.process = server_on_process;
    easy_listen_t *listen;

    if (easy_connection_add_listen(cp.pio, "", cp.port, &cp.phandler) == NULL ||
            (listen = easy_connection_add_listen(cp.sio, "", 0, &cp.shandler)) == NULL) {
        easy_error_log("easy_io_add_listen error, port: %d, %s\n", cp.port, strerror(errno));
        return EASY_ERROR;
    } else {
        easy_inet_myip(&cp.addr);
        cp.addr.port = listen->addr.port;
        easy_error_log("listen start, port = %d, backend port = %d\n", cp.port, cp.addr.port);
    }

    // start
    if (easy_eio_start(cp.pio) || easy_eio_start(cp.cio) || easy_eio_start(cp.sio)) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // 等待线程退出
    easy_eio_wait(cp.pio);
    easy_eio_wait(cp.cio);
    easy_eio_wait(cp.sio);
    easy_eio_destroy(cp.pio);
    easy_eio_destroy(cp.cio);
    easy_eio_destroy(cp.sio);

    return EASY_OK;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -p port\n"
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

static int64_t tonumber(char *ptr)
{
    int64_t datalen = 0;
    char *end = ptr + strlen(ptr) - 1;

    if (*end == 'm' || *end == 'M') {
        datalen = atoi(ptr) * 1024 * 1024;
    } else if (*end == 'k' || *end == 'k') {
        datalen = atoi(ptr) * 1024;
    } else {
        datalen = atoi(ptr);
    }

    return datalen;
}

static int proxy_server_on_process(easy_request_t *r)
{
    int64_t datalen = 16;
    int64_t limit = 0;
    easy_http_request_t *hr = (easy_http_request_t *)r->ipacket;
    char *ptr = (char *)easy_http_get_args(hr, "datalen");

    if (ptr) datalen = tonumber(ptr);

    ptr = (char *)easy_http_get_args(hr, "limit");

    if (ptr) limit = tonumber(ptr);

    if (datalen <= 0) {
        char header[128]; 
        const char *error = "datalen is error. <= 0";
        int hlen = lnprintf(header, 128, "HTTP/1.1 400 Bad Request\r\nContent-Length: %d\r\n\r\n%s", (int)strlen(error), error);
        easy_connection_write_buffer(r->ms->c, header, hlen);
        return EASY_ERROR;
    }

    if (datalen >= (1LL << 32)) {
        easy_connection_write_buffer(r->ms->c, "datalen is error. > 0x7fffffff", -1);
        return EASY_ERROR;
    }

    // send to backend
    easy_session_t          *s;
    easy_simple_packet_t     *packet;

    if ((packet = easy_simple_new(&s, sizeof(datalen))) == NULL)
        return EASY_ERROR;

    easy_session_set_request(s, packet, 5000, NULL);
    packet->data = &packet->buffer[0];
    packet->len = sizeof(datalen);
    packet->chid = easy_atomic32_add_return(&cp.chid, 1);
    *((int64_t *)packet->data) = datalen;

    // dispatch
    easy_request_sleeping(r);
    s->process = proxy_client_on_process;
    easy_session_set_handler(s, &cp.chandler, r);
    easy_session_set_request(s, packet, 5000, (void *)(long)limit);

    if (easy_client_dispatch(cp.cio, cp.addr, s) == EASY_ERROR) {
        easy_session_destroy(s);
        easy_request_sleepless(r);
        easy_error_log("easy_client_dispatch failure.");
        easy_connection_write_buffer(r->ms->c, "easy_client_dispatch failure.", -1);
        return EASY_ERROR;
    }

    return EASY_AGAIN;
}

// 分多次返回
typedef struct test_server_request_t {
    int                   datalen;
    easy_buf_t           *buf;
    easy_simple_packet_t *opacket;
} test_server_request_t;
static int server_on_process(easy_request_t *r)
{
    easy_simple_packet_t     *packet;
    easy_simple_packet_t     *opacket;
    int64_t                  datalen;
    test_server_request_t    *tsr = NULL;
    int                      size;

    packet = (easy_simple_packet_t *)r->ipacket;
    tsr = (test_server_request_t *)r->user_data;

    if (tsr == NULL) {
        if (packet->len != sizeof(datalen)) {
            easy_error_log("server_on_process len is error: %d", packet->len);
            return EASY_ERROR;
        }

        tsr = (test_server_request_t *)easy_pool_calloc(r->ms->pool, sizeof(test_server_request_t));
        r->user_data = tsr;
        datalen = *((int64_t *)packet->data);
        size = easy_min(datalen, 1024 * 1024);
        opacket = easy_simple_rnew(r, size);
        tsr->opacket = opacket;
        tsr->buf = easy_buf_create(r->ms->pool, 0);
        opacket->data = opacket->buffer - sizeof(int) * 2;
        uint32_t *header = (uint32_t *)opacket->data;
        header[0] = datalen;
        header[1] = packet->chid;
        easy_buf_set_data(r->ms->pool, tsr->buf, opacket->data, size + 8);
    } else {
        opacket = tsr->opacket;
        datalen = tsr->datalen;
        size = easy_min(datalen, 1024 * 1024);
        opacket->data = opacket->buffer;
        easy_buf_set_data(r->ms->pool, tsr->buf, opacket->data, size);
    }

    datalen -= size;
    opacket->len = size;
    tsr->datalen = datalen;

    memset(opacket->buffer, 'A', size);
    r->opacket = tsr;

    if (datalen == 0) {
        r->retcode = EASY_OK;
    } else {
        r->retcode = EASY_AGAIN;
    }

    return EASY_OK;
}

static int server_on_simple_encode(easy_request_t *r, void *data)
{
    test_server_request_t *tsr = (test_server_request_t *)data;
    easy_request_addbuf(r, tsr->buf);
    return EASY_OK;
}

typedef struct test_client_response_t {
    int            datalen;
    int            chid;
    easy_session_t *s;
    easy_request_t *cr;
    easy_connection_t *cc;
    int64_t        limit;
    int64_t        start_time;
    int64_t        diff;
} test_client_response_t;

static void *client_on_simple_decode(easy_message_t *m)
{
    int len = 0;

    if ((len = m->input->last - m->input->pos) <= 8)
        return NULL;

    test_client_response_t *tcr = (test_client_response_t *)m->user_data;

    if (tcr == NULL) {
        tcr = (test_client_response_t *)easy_pool_calloc(m->pool, sizeof(test_client_response_t));
        m->user_data = tcr;
        memcpy(tcr, m->input->pos, 8);
        tcr->s = easy_hash_find(m->c->send_queue, tcr->chid);

        if (tcr->s == NULL) {
            easy_error_log("no found packet_id: %d\n", tcr->chid);
            m->status = EASY_ERROR;
            return NULL;
        }

        tcr->cr = (easy_request_t *) tcr->s->r.user_data;
        tcr->limit = (long) tcr->s->r.args;
        tcr->cc = tcr->cr->ms->c;
        char header[128];
        int hlen = lnprintf(header, 128, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", tcr->datalen);
        easy_connection_write_buffer(tcr->cc, header, hlen);
        m->next_read_len = easy_min(tcr->datalen, 1024 * 1024);

        if (tcr->limit > 65536) m->next_read_len = easy_min(m->next_read_len, tcr->limit / 10);

        ev_timer_set(&tcr->s->timeout_watcher, 0.0, 1.0);
    } else {
        tcr->s = easy_hash_find(m->c->send_queue, tcr->chid);

        if (tcr->s == NULL) {
            easy_error_log("no found packet_id: %d\n", tcr->chid);
            m->status = EASY_ERROR;
            return NULL;
        }
    }

    // write data
    len -= 8;
    len = easy_min(tcr->datalen, len);
    tcr->datalen -= len;
    m->input->last = m->input->pos + 8;
    ev_timer_again(m->c->loop, &tcr->s->timeout_watcher);

    if (easy_connection_write_buffer(tcr->cc, m->input->last, len) == EASY_ERROR) {
        m->status = EASY_ERROR;
        return NULL;
    }

    if (tcr->datalen > 0) {
        // QOS
        if (tcr->limit > 0) {
            int64_t end = easy_time_now();

            if (tcr->start_time > 0) {
                int64_t need = 1000000L * len / tcr->limit;
                int64_t cost = end - tcr->start_time;
                tcr->diff += (need - cost);

                if (tcr->diff > 1000) easy_connection_pause(m->c, tcr->diff / 1000);
            }

            tcr->start_time = end;
        }

        return NULL;
    }

    m->input->pos = m->input->last;
    return tcr;
}

static int proxy_client_on_process(easy_request_t *r)
{
    easy_request_t *cr = (easy_request_t *) r->user_data;

    if (r->ipacket == NULL) cr->retcode = EASY_ERROR;

    easy_request_wakeup(cr);
    easy_session_destroy(r->ms);
    return EASY_OK;
}

