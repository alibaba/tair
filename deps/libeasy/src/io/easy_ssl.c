#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>
#include "easy_ssl.h"
#include "easy_log.h"
#include "easy_connection.h"

int                     easy_ssl_connection_index = -1;
easy_atomic_t           *easy_ssl_lock_cs = NULL;
extern __thread easy_baseth_t *easy_baseth_self;
extern char *easy_connection_str(easy_connection_t *c);
static void easy_ssl_info_callback(const SSL *s, int where, int ret);
static int easy_ssl_handshake(easy_connection_t *c);
static void easy_ssl_connection_handshake_handler(easy_connection_t *c);
static void easy_ssl_handshake_handler(struct ev_loop *loop, ev_io *w, int revents);
static void easy_ssl_clear_error();
static void easy_ssl_error(int level, char *fmt, ...);
static int easy_ssl_read(easy_connection_t *c, char *buf, int size, int *pending);
static int easy_ssl_write(easy_connection_t *c, easy_list_t *l);
static void easy_ssl_connection_error(easy_connection_t *c, int sslerr, int err, char *text);
static int easy_ssl_server_create(easy_ssl_t *ssl, easy_ssl_ctx_t *ss);
static int easy_ssl_parse_set_value(easy_ssl_ctx_t *ss, char *key, char *value);
static int easy_ssl_ctx_create(easy_ssl_ctx_t *ssl);
static int easy_ssl_certificate(easy_ssl_ctx_t *ssl, char *cert, char *key);
static int easy_ssl_generate_rsa512_key(easy_ssl_ctx_t *ssl);
static int easy_ssl_dhparam(easy_ssl_ctx_t *ssl, char *file);
static int easy_ssl_session_cache(easy_ssl_ctx_t *ssl, int session_cache, int timeout);
static int easy_ssl_client_certificate(easy_ssl_ctx_t *ssl, char *cert, int depth);
static int easy_ssl_crl(easy_ssl_ctx_t *ssl, char *crl);
static int easy_ssl_handle_recv(easy_connection_t *c, int n);
static void easy_ssl_client_handshake_handler(easy_connection_t *c);
static int easy_ssl_ctx_server_cmp(const void *a, const void *b);
static int easy_ssl_pass_phrase_cb(char *buf, int size, int rwflag, void *conf);

/**
 * 初始化ssl
 */
static unsigned long id_function(void)
{
    return ((unsigned long) pthread_self());
}

static void locking_function(int mode, int type, const char *file, int line)
{
    if (mode & CRYPTO_LOCK) {
        easy_spin_lock(&easy_ssl_lock_cs[type]);
    } else {
        easy_spin_unlock(&easy_ssl_lock_cs[type]);
    }
}
int easy_ssl_init()
{
    if (easy_ssl_connection_index == -1) {
        SSL_library_init();
        SSL_load_error_strings();
        ENGINE_load_builtin_engines();
        OpenSSL_add_all_algorithms();

        easy_ssl_connection_index = SSL_get_ex_new_index(0, NULL, NULL, NULL, NULL);

        if (easy_ssl_connection_index == -1) {
            easy_error_log("SSL_get_ex_new_index() failed");
            return EASY_ERROR;
        }

        int                     num = CRYPTO_num_locks();
        easy_ssl_lock_cs = (easy_atomic_t *)easy_malloc(num * sizeof(easy_atomic_t));
        memset((char *)easy_ssl_lock_cs, 0, num * sizeof(easy_atomic_t));
        CRYPTO_set_id_callback(id_function);
        CRYPTO_set_locking_callback(locking_function);
    }

    return EASY_OK;
}

/**
 * cleanup
 */
int easy_ssl_cleanup()
{
    ENGINE_cleanup();
    EVP_cleanup();
    CRYPTO_cleanup_all_ex_data();
    ERR_remove_state(0);
    ERR_free_strings();
    //SSL_COMP_free();
    //sk_SSL_COMP_free (SSL_COMP_get_compression_methods());
    CRYPTO_mem_leaks_fp(stderr);
    easy_free((char *)easy_ssl_lock_cs);

    return EASY_OK;
}

/**
 * 建立ssl connection
 */
int easy_ssl_connection_create(easy_ssl_ctx_t *ssl, easy_connection_t *c)
{
    easy_ssl_connection_t   *sc;

    sc = easy_pool_calloc(c->pool, sizeof(easy_ssl_connection_t));

    if (sc == NULL) {
        return EASY_ERROR;
    }

    sc->connection = SSL_new(ssl->ctx);

    if (sc->connection == NULL) {
        easy_error_log("SSL_new() failed");
        return EASY_ERROR;
    }

    if (SSL_set_fd(sc->connection, c->fd) == 0) {
        easy_error_log("SSL_set_fd() failed");
        return EASY_ERROR;
    }

    if (c->type == EASY_TYPE_CLIENT) {
        SSL_set_connect_state(sc->connection);
    } else {
        SSL_set_accept_state(sc->connection);
    }

    if (SSL_set_ex_data(sc->connection, easy_ssl_connection_index, c) == 0) {
        easy_error_log("SSL_set_ex_data() failed");
        return EASY_ERROR;
    }

    sc->session_reuse = ssl->conf.session_reuse;
    c->sc = sc;

    return EASY_OK;
}

int easy_ssl_connection_destroy(easy_connection_t *c)
{
    if (c->sc) {
        int                     n, mode;

        mode = SSL_RECEIVED_SHUTDOWN | SSL_SENT_SHUTDOWN;
        SSL_set_shutdown(c->sc->connection, mode);
        easy_ssl_clear_error();
        n = SSL_shutdown(c->sc->connection);

        if (n != 1 && ERR_peek_error()) {
            SSL_get_error(c->sc->connection, n);
        }

        SSL_free(c->sc->connection);

        c->sc = NULL;
    }

    return EASY_OK;
}

/**
 * 握手
 */
void easy_ssl_connection_handshake(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;
    int                     n, rc;
    char                    buf[1];

    c = (easy_connection_t *)w->data;
    assert(c->fd == w->fd);

    easy_debug_log("easy_ssl_connection_handshake: %s", easy_connection_str(c));

    n = recv(c->fd, (char *) buf, 1, MSG_PEEK);

    if (n <= 0) {
        easy_debug_log("%s n: %d, error: %s(%d)\n", easy_connection_str(c), n, strerror(errno), errno);
        c->conn_has_error = (n < 0 ? 1 : 0);
        goto error_exit;
    }

    if (!((buf[0] & 0x80) || (buf[0] == 0x16)))
        goto error_exit;

    easy_debug_log("ssl handshake: 0x%02Xd", buf[0]);

    rc = easy_ssl_handshake(c);

    if (rc == EASY_ERROR) {
        goto error_exit;
    } else if (rc == EASY_AGAIN) {
        c->sc->handler = easy_ssl_connection_handshake_handler;
    } else {
        easy_ssl_connection_handshake_handler(c);
    }

    return;
error_exit:
    easy_connection_destroy(c);
}

/**
 * client握手
 */
int easy_ssl_client_do_handshake(easy_connection_t *c)
{
    int                     rc;
    uint64_t                key;
    easy_ssl_ctx_t          *ctx;
    char                    *servername;
    easy_ssl_ctx_server_t   *cs;
    easy_ssl_t              *ssl = easy_baseth_self->eio->ssl;

    servername = c->client->server_name;
    ctx = ssl->client_ctx;

    if (servername) {
        key = easy_hash_code(servername, strlen(servername), 3);
        cs = (easy_ssl_ctx_server_t *)easy_hash_find_ex(ssl->client_map, key, easy_ssl_ctx_server_cmp, servername);

        if (cs) {
            ctx = cs->ss;
        }
    }

    if (easy_ssl_connection_create(ctx, c) != EASY_OK) {
        easy_error_log("easy_ssl_connection_create\n");
        return EASY_ERROR;
    }

    // reuse
    if (c->sc->session_reuse && c->client->ssl_session) {
        if (SSL_set_session(c->sc->connection, c->client->ssl_session) == 0) {
            easy_error_log("SSL_set_session() failed");
            return EASY_ERROR;
        }
    }

    // handshake
    rc = easy_ssl_handshake(c);

    if (rc == EASY_ERROR) {
        return EASY_ERROR;
    } else if (rc == EASY_AGAIN) {
        c->sc->handler = easy_ssl_client_handshake_handler;
    } else {
        easy_ssl_client_handshake_handler(c);
    }

    return EASY_OK;
}
void easy_ssl_client_handshake(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;

    c = (easy_connection_t *)w->data;

    if (easy_ssl_client_do_handshake(c) != EASY_OK) {
        easy_error_log("easy_ssl_client_handshake failed");
        easy_connection_destroy(c);
    }
}
///////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * ssl info callback
 */
static void easy_ssl_info_callback(const SSL *s, int where, int ret)
{
    easy_connection_t       *c;

    if (where & SSL_CB_HANDSHAKE_START) {
        c = easy_ssl_get_connection(s);

        if (c->sc->handshaked) {
            c->sc->renegotiation = 1;
        }
    }
}
/**
 * easy_ssl_handshake
 */
static int easy_ssl_handshake(easy_connection_t *c)
{
    int                     n, sslerr, err;

    easy_ssl_clear_error();
    n = SSL_do_handshake(c->sc->connection);
    easy_debug_log("SSL_do_handshake: %d", n);

    if (n == 1) {
        ev_io_start(c->loop, &c->read_watcher);
        ev_io_start(c->loop, &c->write_watcher);
        c->sc->handshaked = 1;
        c->read = easy_ssl_read;
        c->write = easy_ssl_write;

        if (c->sc->connection->s3) {
            c->sc->connection->s3->flags |= SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS;
        }

        return EASY_OK;
    }

    sslerr = SSL_get_error(c->sc->connection, n);
    easy_debug_log("SSL_get_error: %d", sslerr);

    if (sslerr == SSL_ERROR_WANT_READ) {
        ev_set_cb(&c->read_watcher, easy_ssl_handshake_handler);
        ev_set_cb(&c->write_watcher, easy_ssl_handshake_handler);
        ev_io_start(c->loop, &c->read_watcher);
        ev_io_stop(c->loop, &c->write_watcher);

        return EASY_AGAIN;
    }

    if (sslerr == SSL_ERROR_WANT_WRITE) {
        ev_set_cb(&c->read_watcher, easy_ssl_handshake_handler);
        ev_set_cb(&c->write_watcher, easy_ssl_handshake_handler);
        ev_io_start(c->loop, &c->write_watcher);
        ev_io_stop(c->loop, &c->read_watcher);

        return EASY_AGAIN;
    }

    err = (sslerr == SSL_ERROR_SYSCALL) ? errno : 0;

    if (sslerr == SSL_ERROR_ZERO_RETURN || ERR_peek_error() == 0) {
        easy_error_log("peer closed connection in SSL handshake");
        return EASY_ERROR;
    }

    easy_ssl_connection_error(c, sslerr, err, "SSL_do_handshake() failed");

    return EASY_ERROR;
}

/**
 * clear error
 */
static void easy_ssl_clear_error()
{
    while (ERR_peek_error()) {
        easy_ssl_error(EASY_LOG_INFO, "ignoring stale global SSL error");
    }

    ERR_clear_error();
}

/**
 * print ssl error
 */
#define EASY_MAX_CONF_ERRSTR 1024
static void easy_ssl_error(int level, char *fmt, ...)
{
    uint64_t                n;
    va_list                 args;
    char                    *p, *last;
    char                    errstr[EASY_MAX_CONF_ERRSTR];

    va_start(args, fmt);
    p = errstr + easy_vsnprintf(errstr, EASY_MAX_CONF_ERRSTR, fmt, args);
    va_end(args);
    last = errstr + EASY_MAX_CONF_ERRSTR;

    for ( ;; ) {
        n = ERR_get_error();

        if (n == 0) {
            break;
        }

        if (p >= last) {
            continue;
        }

        *p++ = ' ';

        ERR_error_string_n(n, (char *) p, last - p);

        while (p < last && *p) {
            p++;
        }
    }

    easy_log_format(level, __FILE__, __LINE__, __FUNCTION__, "%s", errstr);
}

/**
 * easy_ssl_connection_error
 */
static void easy_ssl_connection_error(easy_connection_t *c, int sslerr, int err, char *text)
{
    int                     n;
    int                     level;

    level = EASY_LOG_ERROR;

    if (sslerr == SSL_ERROR_SYSCALL) {

        if (err == ECONNRESET
                || err == EPIPE
                || err == ENOTCONN
                || err == ETIMEDOUT
                || err == ECONNREFUSED
                || err == ENETDOWN
                || err == ENETUNREACH
                || err == EHOSTDOWN
                || err == EHOSTUNREACH) {
            level = EASY_LOG_INFO;
        }

    } else if (sslerr == SSL_ERROR_SSL) {

        n = ERR_GET_REASON(ERR_peek_error());

        /* handshake failures */
        if (n == SSL_R_BLOCK_CIPHER_PAD_IS_WRONG                     /*  129 */
                || n == SSL_R_DIGEST_CHECK_FAILED                        /*  149 */
                || n == SSL_R_LENGTH_MISMATCH                            /*  159 */
                || n == SSL_R_NO_CIPHERS_PASSED                          /*  182 */
                || n == SSL_R_NO_CIPHERS_SPECIFIED                       /*  183 */
                || n == SSL_R_NO_SHARED_CIPHER                           /*  193 */
                || n == SSL_R_RECORD_LENGTH_MISMATCH                     /*  213 */
                || n == SSL_R_UNEXPECTED_MESSAGE                         /*  244 */
                || n == SSL_R_UNEXPECTED_RECORD                          /*  245 */
                || n == SSL_R_UNKNOWN_ALERT_TYPE                         /*  246 */
                || n == SSL_R_UNKNOWN_PROTOCOL                           /*  252 */
                || n == SSL_R_WRONG_VERSION_NUMBER                       /*  267 */
                || n == SSL_R_DECRYPTION_FAILED_OR_BAD_RECORD_MAC        /*  281 */
                || n == 1000 /* SSL_R_SSLV3_ALERT_CLOSE_NOTIFY */
                || n == SSL_R_SSLV3_ALERT_UNEXPECTED_MESSAGE             /* 1010 */
                || n == SSL_R_SSLV3_ALERT_BAD_RECORD_MAC                 /* 1020 */
                || n == SSL_R_TLSV1_ALERT_DECRYPTION_FAILED              /* 1021 */
                || n == SSL_R_TLSV1_ALERT_RECORD_OVERFLOW                /* 1022 */
                || n == SSL_R_SSLV3_ALERT_DECOMPRESSION_FAILURE          /* 1030 */
                || n == SSL_R_SSLV3_ALERT_HANDSHAKE_FAILURE              /* 1040 */
                || n == SSL_R_SSLV3_ALERT_NO_CERTIFICATE                 /* 1041 */
                || n == SSL_R_SSLV3_ALERT_BAD_CERTIFICATE                /* 1042 */
                || n == SSL_R_SSLV3_ALERT_UNSUPPORTED_CERTIFICATE        /* 1043 */
                || n == SSL_R_SSLV3_ALERT_CERTIFICATE_REVOKED            /* 1044 */
                || n == SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED            /* 1045 */
                || n == SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN            /* 1046 */
                || n == SSL_R_SSLV3_ALERT_ILLEGAL_PARAMETER              /* 1047 */
                || n == SSL_R_TLSV1_ALERT_UNKNOWN_CA                     /* 1048 */
                || n == SSL_R_TLSV1_ALERT_ACCESS_DENIED                  /* 1049 */
                || n == SSL_R_TLSV1_ALERT_DECODE_ERROR                   /* 1050 */
                || n == SSL_R_TLSV1_ALERT_DECRYPT_ERROR                  /* 1051 */
                || n == SSL_R_TLSV1_ALERT_EXPORT_RESTRICTION             /* 1060 */
                || n == SSL_R_TLSV1_ALERT_PROTOCOL_VERSION               /* 1070 */
                || n == SSL_R_TLSV1_ALERT_INSUFFICIENT_SECURITY          /* 1071 */
                || n == SSL_R_TLSV1_ALERT_INTERNAL_ERROR                 /* 1080 */
                || n == SSL_R_TLSV1_ALERT_USER_CANCELLED                 /* 1090 */
                || n == SSL_R_TLSV1_ALERT_NO_RENEGOTIATION) {            /* 1100 */
            level = EASY_LOG_INFO;
        }
    }

    easy_ssl_error(level, text);
}

static void easy_ssl_connection_handshake_handler(easy_connection_t *c)
{
    if (c->sc->handshaked) {
        ev_set_cb(&c->read_watcher, easy_connection_on_readable);
        ev_set_cb(&c->write_watcher, easy_connection_on_writable);
    }
}

static void easy_ssl_handshake_handler(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;
    int                     rc;

    c = w->data;
    easy_debug_log("easy_ssl_handshake_handler: %s", easy_connection_str(c));
    rc = easy_ssl_handshake(c);

    if (rc == EASY_AGAIN) {
        return;
    }

    c->sc->handler(c);

    if (rc == EASY_ERROR)
        easy_connection_destroy(c);
}

static int easy_ssl_read(easy_connection_t *c, char *buf, int size, int *pending)
{
    int                     n, bytes;

    if (c->sc->last == EASY_ERROR) {
        return EASY_ERROR;
    }

    if (c->sc->last == EASY_ABORT) {
        return 0;
    }

    easy_ssl_clear_error();

    bytes = 0;

    for ( ;; ) {
        n = SSL_read(c->sc->connection, buf, size);

        if (n > 0) {
            bytes += n;
        }

        c->sc->last = easy_ssl_handle_recv(c, n);

        if (c->sc->last == EASY_OK) {
            size -= n;

            if (size == 0) {
                *pending = SSL_pending(c->sc->connection);
                return bytes;
            }

            buf += n;
            continue;
        }

        if (bytes) {
            return bytes;
        }

        if (c->sc->last == EASY_ABORT) {
            return 0;
        } else if (c->sc->last == EASY_ERROR || c->sc->last == EASY_AGAIN) {
            return c->sc->last;
        }
    }

    return bytes;
}

static int easy_ssl_handle_recv(easy_connection_t *c, int n)
{
    int                     sslerr;
    int                     err;

    if (c->sc->renegotiation) {
        easy_error_log("SSL renegotiation disabled");
        return EASY_ERROR;
    }

    if (n > 0) {
        return EASY_OK;
    }

    sslerr = SSL_get_error(c->sc->connection, n);
    err = (sslerr == SSL_ERROR_SYSCALL) ? errno : 0;
    easy_debug_log("SSL_get_error: %d", sslerr);

    if (sslerr == SSL_ERROR_WANT_READ) {
        return EASY_AGAIN;
    }

    if (sslerr == SSL_ERROR_WANT_WRITE) {
        easy_error_log("peer started SSL renegotiation");

        ev_set_cb(&c->read_watcher, easy_ssl_handshake_handler);
        ev_set_cb(&c->write_watcher, easy_ssl_handshake_handler);
        ev_io_start(c->loop, &c->write_watcher);
        return EASY_AGAIN;
    }

    if (sslerr == SSL_ERROR_ZERO_RETURN || ERR_peek_error() == 0) {
        easy_debug_log("peer shutdown SSL cleanly");
        return EASY_ABORT;
    }

    easy_ssl_connection_error(c, sslerr, err, "SSL_read() failed");
    return EASY_ERROR;
}

static int easy_ssl_write(easy_connection_t *c, easy_list_t *l)
{
    easy_buf_t              *b, *b1;
    int                     bytes, ret, size, sslerr;

    // foreach
    bytes = 0;
    easy_list_for_each_entry_safe(b, b1, l, node) {
        size = b->last - b->pos;
        ret = SSL_write(c->sc->connection, b->pos, size);

        if (ret <= 0) {
            sslerr = SSL_get_error(c->sc->connection, ret);

            if (sslerr == SSL_ERROR_WANT_WRITE || sslerr == SSL_ERROR_WANT_READ) {
                return bytes;
            } else {
                return EASY_ERROR;
            }
        }

        b->pos += ret;
        bytes += ret;

        if (ret < size)
            break;

        easy_buf_destroy(b);
    }
    return bytes;
}


///////////////////////////////////////////////////////////////////////////////////////////////////
// load ssl config file
easy_ssl_t *easy_ssl_config_load(char *filename)
{
    FILE                    *fp = NULL;
    easy_pool_t             *pool = NULL;
    easy_ssl_ctx_t          *ss = NULL;
    easy_ssl_t              *ssl;
    char                    buffer[1024], *p, *end, *value;
    int                     status = 0;
    int                     line = 0;

    if ((fp = fopen(filename, "rb")) == NULL) {
        easy_error_log("%s not open.", filename);
        return NULL;
    }

    if ((pool = easy_pool_create(1024)) == NULL) {
        goto error_exit;
    }

    if ((ssl = (easy_ssl_t *)easy_pool_calloc(pool, sizeof(easy_ssl_t))) == NULL) {
        goto error_exit;
    }

    ssl->pool = pool;
    ssl->server_map = easy_hash_create(pool, 128, offsetof(easy_ssl_ctx_server_t, node));
    ssl->client_map = easy_hash_create(pool, 128, offsetof(easy_ssl_ctx_server_t, node));
    easy_list_init(&ssl->server_list);

    // 读入
    while(fgets(buffer, 1024, fp)) {
        line ++;
        p = buffer;

        while(*p && *p <= ' ') {
            p ++;
            continue;
        }

        if (*p == '\0' || *p == '#') continue;

        // status
        if (status == 0) {
            end = strchr(p, '{');

            if (end) {
                *end = '\0';
                status = 1;
                ss = easy_pool_calloc(pool, sizeof(easy_ssl_ctx_t));

                if (ss == NULL) {
                    goto error_exit;
                }

                ss->pool = pool;
                ss->conf.session_timeout = 300;
                ss->conf.verify_depth = 1;
                ss->conf.file = filename;
                ss->conf.line = line;
                ss->type = (strncmp(p, "client", 6) == 0 ? 1 : 0);
            }
        } else {
            end = strchr(p, '}');

            if (end) {
                *end = '\0';
                status = 0;

                if (easy_ssl_server_create(ssl, ss) == EASY_ERROR) {
                    goto error_exit;
                }
            }

            end = strrchr(p, ';');

            if (!end) {
                if (status == 0) continue;

                easy_error_log("Line %d at %s Error", line, filename);
                goto error_exit;
            }

            *end = '\0';
            value = strchr(p, ' ');

            if (!value) {
                easy_error_log("Line %d at %s Error", line, filename);
                goto error_exit;
            }

            *value ++ = '\0';

            while(*value && *value <= ' ') {
                value ++;
                continue;
            }

            if (easy_ssl_parse_set_value(ss, p, value) == EASY_ERROR) {
                easy_error_log("key: %s Line %d at %s Error", p, line, filename);
                goto error_exit;
            }
        }
    }

    if (status != 0) {
        easy_error_log("Line %d at %s Error", line, filename);
        goto error_exit;
    }

    // client default
    if (ssl->client_ctx == NULL) {
        ss = easy_pool_calloc(pool, sizeof(easy_ssl_ctx_t));

        if (ss == NULL) {
            goto error_exit;
        }

        ss->pool = pool;
        ss->type = 1;

        if (easy_ssl_server_create(ssl, ss) == EASY_ERROR) {
            goto error_exit;
        }

        ssl->client_ctx = ss;
    }

    fclose(fp);
    return ssl;
error_exit:

    if (fp) fclose(fp);

    if (pool) easy_pool_destroy(pool);

    return NULL;
}

int easy_ssl_config_destroy(easy_ssl_t *ssl)
{
    easy_ssl_ctx_t          *ss;

    if (ssl) {
        easy_list_for_each_entry(ss, &ssl->server_list, list_node) {
            easy_debug_log("destroy ssl->ctx: %p", ss->ctx);
            SSL_CTX_free(ss->ctx);
        }
        easy_pool_destroy(ssl->pool);
    }

    return EASY_OK;
}

static int easy_ssl_parse_set_value(easy_ssl_ctx_t *ss, char *key, char *value)
{
    if (*key == '\0' || *value == '\0')
        return EASY_ERROR;

    if (strcmp(key, "ssl_certificate") == 0) {
        ss->conf.certificate = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_certificate_key") == 0) {
        ss->conf.certificate_key = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_dhparam") == 0) {
        ss->conf.dhparam = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_client_certificate") == 0) {
        ss->conf.client_certificate = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_crl") == 0) {
        ss->conf.crl = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_pass_phrase_dialog") == 0) {
        ss->conf.pass_phrase_dialog = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_ciphers") == 0) {
        ss->conf.ciphers = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "server_name") == 0) {
        ss->conf.server_name = easy_pool_strdup(ss->pool, value);
    } else if (strcmp(key, "ssl_prefer_server_ciphers") == 0) {
        if (strcasecmp(value, "on") == 0) {
            ss->conf.prefer_server_ciphers = 1;
        } else if (strcasecmp(value, "off") == 0) {
            ss->conf.prefer_server_ciphers = 0;
        } else {
            return EASY_ERROR;
        }
    } else if (strcmp(key, "ssl_verify") == 0) {
        ss->conf.verify = strtol(value, (char **)NULL, 10);
    } else if (strcmp(key, "ssl_verify_depth") == 0) {
        ss->conf.verify_depth = strtol(value, (char **)NULL, 10);
    } else if (strcmp(key, "ssl_session_timeout") == 0) {
        ss->conf.session_timeout = strtol(value, (char **)NULL, 10);
    } else if (strcmp(key, "ssl_session_cache") == 0) {
        if (strcasecmp(value, "off") == 0) {
            ss->conf.session_cache = EASY_SSL_SCACHE_OFF;
        } else if (strcasecmp(value, "builtin") == 0) {
            ss->conf.session_cache = EASY_SSL_SCACHE_BUILTIN;
        } else {
            return EASY_ERROR;
        }
    } else if (strcmp(key, "ssl_protocols") == 0) {
        ss->conf.protocols = 0;

        if (strstr(value, "SSLv2") == NULL) {
            ss->conf.protocols |= SSL_OP_NO_SSLv2;
        } else if (strstr(value, "SSLv3") == NULL) {
            ss->conf.protocols |= SSL_OP_NO_SSLv3;
        } else if (strstr(value, "TLSv1") == NULL) {
            ss->conf.protocols |= SSL_OP_NO_TLSv1;
        }
    } else if (strcmp(key, "ssl_session_reuse") == 0) {
        ss->conf.session_reuse = (strcasecmp(value, "on") == 0 ? 1 : 0);
    } else {
        return EASY_ERROR;
    }

    return EASY_OK;
}

/**
 * 对ctx_server的比较
 */
static int easy_ssl_ctx_server_cmp(const void *a, const void *b)
{
    easy_ssl_ctx_server_t   *cs = (easy_ssl_ctx_server_t *) b;
    return strcmp(cs->server_name, (const char *)a);
}

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
int easy_ssl_servername(SSL *ssl_conn, int *ad, void *arg)
{
    const char               *servername;
    size_t                    len;
    char                     *p, host[128];
    easy_ssl_ctx_server_t    *cs;
    easy_ssl_t               *ssl = easy_baseth_self->eio->ssl;

    servername = SSL_get_servername(ssl_conn, TLSEXT_NAMETYPE_host_name);

    if (ssl == NULL || servername == NULL) {
        return SSL_TLSEXT_ERR_NOACK;
    }

    if ((len = strlen(servername)) == 0) {
        return SSL_TLSEXT_ERR_NOACK;
    }

    if ((p = strchr(servername, ':')) != NULL) {
        len = p - servername;
    }

    len = easy_min(127, len);

    if (len <= 0) {
        return SSL_TLSEXT_ERR_NOACK;
    }

    memcpy(host, servername, len);
    host[len] = '\0';
    uint64_t                key = easy_hash_code(host, len, 3);
    cs = (easy_ssl_ctx_server_t *)easy_hash_find_ex(ssl->server_map, key, easy_ssl_ctx_server_cmp, host);

    if (cs == NULL) {
        return SSL_TLSEXT_ERR_NOACK;
    }

    SSL_set_SSL_CTX(ssl_conn, cs->ss->ctx);

    return SSL_TLSEXT_ERR_OK;
}
#endif

/**
 * 初始化server ctx, 并加入hash_map中
 */
static int easy_ssl_server_create(easy_ssl_t *ssl, easy_ssl_ctx_t *ss)
{

    if (ss->type) {
        // 建立ctx
        if (easy_ssl_ctx_create(ss) != EASY_OK) {
            return EASY_ERROR;
        }

        if (ss->conf.certificate) {
            char                    *key = ss->conf.certificate_key ? ss->conf.certificate_key : ss->conf.certificate;

            if (easy_ssl_certificate(ss, ss->conf.certificate, key) != EASY_OK) {
                return EASY_ERROR;
            }
        }

        if (!ssl->client_ctx) ssl->client_ctx = ss;
    } else {
        if (!ss->conf.certificate) {
            easy_error_log("no \"ssl_certificate\" is defined in %s:%d", ss->conf.file, ss->conf.line);
            return EASY_ERROR;
        }

        if (!ss->conf.certificate_key) {
            easy_error_log("no \"ssl_certificate_key\" is defined in %s:%d", ss->conf.file, ss->conf.line);
            return EASY_ERROR;
        }

        if (!ss->conf.server_name) {
            easy_error_log("no \"server_name\" is defined in %s:%d", ss->conf.file, ss->conf.line);
            return EASY_ERROR;
        }

        // 建立ctx
        if (easy_ssl_ctx_create(ss) != EASY_OK) {
            return EASY_ERROR;
        }

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME

        if (SSL_CTX_set_tlsext_servername_callback(ss->ctx, easy_ssl_servername) == 0) {
            easy_warn_log("SNI is not available");
        }

#endif

        if (easy_ssl_certificate(ss, ss->conf.certificate, ss->conf.certificate_key) != EASY_OK) {
            return EASY_ERROR;
        }

        if (SSL_CTX_set_cipher_list(ss->ctx, (const char *) ss->conf.ciphers) == 0) {
            easy_error_log("SSL_CTX_set_cipher_list(\"%V\") failed", ss->conf.ciphers);
        }

        if (ss->conf.verify) {
            if (!ss->conf.client_certificate ) {
                easy_error_log("no ssl_client_certificate for ssl_client_verify");
                return EASY_ERROR;
            }

            if (easy_ssl_client_certificate(ss, ss->conf.client_certificate, ss->conf.verify_depth) != EASY_OK) {
                return EASY_ERROR;
            }

            if (easy_ssl_crl(ss, ss->conf.crl) != EASY_OK) {
                return EASY_ERROR;
            }
        }

        if (ss->conf.prefer_server_ciphers) {
            SSL_CTX_set_options(ss->ctx, SSL_OP_CIPHER_SERVER_PREFERENCE);
        }

        if (easy_ssl_generate_rsa512_key(ss) != EASY_OK) {
            return EASY_ERROR;
        }

        if (easy_ssl_dhparam(ss, ss->conf.dhparam) != EASY_OK) {
            return EASY_ERROR;
        }

        if (easy_ssl_session_cache(ss, ss->conf.session_cache, ss->conf.session_timeout) != EASY_OK) {
            return EASY_ERROR;
        }

        // add
        if (ssl->server_ctx == NULL) ssl->server_ctx = ss;
    }

    easy_ssl_ctx_server_t   *cs;
    char                    *p, *q;
    uint64_t                key;

    p = ss->conf.server_name;

    while(p && *p) {
        if ((q = strchr(p, ' ')) != NULL)
            * q = '\0';

        key = easy_hash_code(p, strlen(p), 3);

        if (ss->type) {
            cs = (easy_ssl_ctx_server_t *)easy_hash_find_ex(ssl->client_map, key, easy_ssl_ctx_server_cmp, p);
        } else {
            cs = (easy_ssl_ctx_server_t *)easy_hash_find_ex(ssl->server_map, key, easy_ssl_ctx_server_cmp, p);
        }

        if (cs == NULL) {
            cs = (easy_ssl_ctx_server_t *)easy_pool_calloc(ssl->pool, sizeof(easy_ssl_ctx_server_t));
            cs->server_name = p;
            cs->ss = ss;

            if (ss->type) {
                easy_hash_add(ssl->client_map, key, &cs->node);
            } else {
                easy_hash_add(ssl->server_map, key, &cs->node);
            }
        }

        p = q;
    }

    easy_list_add_tail(&ss->list_node, &ssl->server_list);

    return EASY_OK;
}

/**
 * create ssl ctx
 */
static int easy_ssl_ctx_create(easy_ssl_ctx_t *ssl)
{
    ssl->ctx = SSL_CTX_new(SSLv23_method());
    easy_debug_log("create ssl->ctx: %p", ssl->ctx);

    if (ssl->ctx == NULL) {
        easy_error_log("SSL_CTX_new() failed");
        return EASY_ERROR;
    }

    /* client side options */
    SSL_CTX_set_options(ssl->ctx, SSL_OP_MICROSOFT_SESS_ID_BUG);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_NETSCAPE_CHALLENGE_BUG);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG);

    /* server side options */
    SSL_CTX_set_options(ssl->ctx, SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER);

    /* this option allow a potential SSL 2.0 rollback (CAN-2005-2969) */
    SSL_CTX_set_options(ssl->ctx, SSL_OP_MSIE_SSLV2_RSA_PADDING);

    SSL_CTX_set_options(ssl->ctx, SSL_OP_SSLEAY_080_CLIENT_DH_BUG);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_TLS_D5_BUG);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_TLS_BLOCK_PADDING_BUG);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);
    SSL_CTX_set_options(ssl->ctx, SSL_OP_SINGLE_DH_USE);

    if (ssl->conf.protocols) SSL_CTX_set_options(ssl->ctx, ssl->conf.protocols);

    SSL_CTX_set_read_ahead(ssl->ctx, 1);
    SSL_CTX_set_info_callback(ssl->ctx, easy_ssl_info_callback);

    return EASY_OK;
}

static int easy_ssl_certificate(easy_ssl_ctx_t *ssl, char *cert, char *key)
{
    if (ssl->type) {
        if (SSL_CTX_use_certificate_file(ssl->ctx, cert, SSL_FILETYPE_PEM) <= 0) {
            easy_error_log("SSL_CTX_use_certificate_file(\"%s\") failed", cert);
            return EASY_ERROR;
        }
    } else {
        if (SSL_CTX_use_certificate_chain_file(ssl->ctx, cert) <= 0) {
            easy_error_log("SSL_CTX_use_certificate_chain_file(\"%s\") failed", cert);
            return EASY_ERROR;
        }
    }

    easy_ssl_pass_phrase_dialog_t dialog;
    memset(&dialog, 0, sizeof(dialog));
    dialog.type = ssl->conf.pass_phrase_dialog;
    dialog.server_name = ssl->conf.server_name;
    SSL_CTX_set_default_passwd_cb_userdata(ssl->ctx, &dialog);
    SSL_CTX_set_default_passwd_cb(ssl->ctx, easy_ssl_pass_phrase_cb);

    if (SSL_CTX_use_PrivateKey_file(ssl->ctx, key, SSL_FILETYPE_PEM) <= 0) {
        easy_error_log("SSL_CTX_use_PrivateKey_file(\"%s\") failed", key);
        return EASY_ERROR;
    }

    return EASY_OK;
}

static int easy_ssl_generate_rsa512_key(easy_ssl_ctx_t *ssl)
{
    RSA                     *key;

    if (SSL_CTX_need_tmp_RSA(ssl->ctx) == 0) {
        return EASY_OK;
    }

    key = RSA_generate_key(512, RSA_F4, NULL, NULL);

    if (key) {
        SSL_CTX_set_tmp_rsa(ssl->ctx, key);

        RSA_free(key);

        return EASY_OK;
    }

    easy_ssl_error(EASY_LOG_ERROR, "RSA_generate_key(512) failed");

    return EASY_ERROR;
}

static int easy_ssl_dhparam(easy_ssl_ctx_t *ssl, char *file)
{
    DH                      *dh;
    BIO                     *bio;

    /*
     * -----BEGIN DH PARAMETERS-----
     * MIGHAoGBALu8LcrYRnSQfEP89YDpz9vZWKP1aLQtSwju1OsPs1BMbAMCducQgAxc
     * y7qokiYUxb7spWWl/fHSh6K8BJvmd4Bg6RqSp1fjBI9osHb302zI8pul34HcLKcl
     * 7OZicMyaUDXYzs7vnqAnSmOrHlj6/UmI0PZdFGdX2gcd8EXP4WubAgEC
     * -----END DH PARAMETERS-----
     */

    static unsigned char    dh1024_p[] = {
        0xBB, 0xBC, 0x2D, 0xCA, 0xD8, 0x46, 0x74, 0x90, 0x7C, 0x43, 0xFC, 0xF5,
        0x80, 0xE9, 0xCF, 0xDB, 0xD9, 0x58, 0xA3, 0xF5, 0x68, 0xB4, 0x2D, 0x4B,
        0x08, 0xEE, 0xD4, 0xEB, 0x0F, 0xB3, 0x50, 0x4C, 0x6C, 0x03, 0x02, 0x76,
        0xE7, 0x10, 0x80, 0x0C, 0x5C, 0xCB, 0xBA, 0xA8, 0x92, 0x26, 0x14, 0xC5,
        0xBE, 0xEC, 0xA5, 0x65, 0xA5, 0xFD, 0xF1, 0xD2, 0x87, 0xA2, 0xBC, 0x04,
        0x9B, 0xE6, 0x77, 0x80, 0x60, 0xE9, 0x1A, 0x92, 0xA7, 0x57, 0xE3, 0x04,
        0x8F, 0x68, 0xB0, 0x76, 0xF7, 0xD3, 0x6C, 0xC8, 0xF2, 0x9B, 0xA5, 0xDF,
        0x81, 0xDC, 0x2C, 0xA7, 0x25, 0xEC, 0xE6, 0x62, 0x70, 0xCC, 0x9A, 0x50,
        0x35, 0xD8, 0xCE, 0xCE, 0xEF, 0x9E, 0xA0, 0x27, 0x4A, 0x63, 0xAB, 0x1E,
        0x58, 0xFA, 0xFD, 0x49, 0x88, 0xD0, 0xF6, 0x5D, 0x14, 0x67, 0x57, 0xDA,
        0x07, 0x1D, 0xF0, 0x45, 0xCF, 0xE1, 0x6B, 0x9B
    };

    static unsigned char    dh1024_g[] = { 0x02 };

    if (!file) {

        dh = DH_new();

        if (dh == NULL) {
            easy_ssl_error(EASY_LOG_ERROR, "DH_new() failed");
            return EASY_ERROR;
        }

        dh->p = BN_bin2bn(dh1024_p, sizeof(dh1024_p), NULL);
        dh->g = BN_bin2bn(dh1024_g, sizeof(dh1024_g), NULL);

        if (dh->p == NULL || dh->g == NULL) {
            easy_ssl_error(EASY_LOG_ERROR, "BN_bin2bn() failed");
            DH_free(dh);
            return EASY_ERROR;
        }

        SSL_CTX_set_tmp_dh(ssl->ctx, dh);

        DH_free(dh);

        return EASY_OK;
    }

    bio = BIO_new_file(file, "r");

    if (bio == NULL) {
        easy_ssl_error(EASY_LOG_ERROR, "BIO_new_file(\"%s\") failed", file);
        return EASY_ERROR;
    }

    dh = PEM_read_bio_DHparams(bio, NULL, NULL, NULL);

    if (dh == NULL) {
        easy_ssl_error(EASY_LOG_ERROR, "PEM_read_bio_DHparams(\"%s\") failed", file);
        BIO_free(bio);
        return EASY_ERROR;
    }

    SSL_CTX_set_tmp_dh(ssl->ctx, dh);

    DH_free(dh);
    BIO_free(bio);

    return EASY_OK;
}

static int easy_ssl_session_cache(easy_ssl_ctx_t *ssl, int session_cache, int timeout)
{
    if (session_cache == EASY_SSL_SCACHE_OFF) {
        SSL_CTX_set_session_cache_mode(ssl->ctx, SSL_SESS_CACHE_OFF);
    } else {
        //SSL_CTX_set_session_id_context(ssl->ctx, sess_ctx->data, sess_ctx->len);
        SSL_CTX_set_session_cache_mode(ssl->ctx, SSL_SESS_CACHE_SERVER);
        SSL_CTX_set_timeout(ssl->ctx, (long) timeout);
    }

    return EASY_OK;
}

static int easy_ssl_verify_callback(int ok, X509_STORE_CTX *x509_store)
{
    return 1;
}

static int easy_ssl_client_certificate(easy_ssl_ctx_t *ssl, char *cert, int depth)
{
    STACK_OF(X509_NAME)  *list;

    SSL_CTX_set_verify(ssl->ctx, SSL_VERIFY_PEER, easy_ssl_verify_callback);

    SSL_CTX_set_verify_depth(ssl->ctx, depth);

    if (!cert) {
        return EASY_OK;
    }

    if (SSL_CTX_load_verify_locations(ssl->ctx, cert, NULL) == 0) {
        easy_ssl_error(EASY_LOG_ERROR, "SSL_CTX_load_verify_locations(\"%s\") failed", cert);
        return EASY_ERROR;
    }

    if ((list = SSL_load_client_CA_file(cert)) == NULL) {
        easy_ssl_error(EASY_LOG_ERROR, "SSL_load_client_CA_file(\"%s\") failed", cert);
        return EASY_ERROR;
    }

    ERR_clear_error();

    SSL_CTX_set_client_CA_list(ssl->ctx, list);

    return EASY_OK;
}

static int easy_ssl_crl(easy_ssl_ctx_t *ssl, char *crl)
{
    X509_STORE              *store;
    X509_LOOKUP             *lookup;

    if (!crl) {
        return EASY_OK;
    }

    store = SSL_CTX_get_cert_store(ssl->ctx);

    if (store == NULL) {
        easy_ssl_error(EASY_LOG_ERROR, "SSL_CTX_get_cert_store() failed");
        return EASY_ERROR;
    }

    lookup = X509_STORE_add_lookup(store, X509_LOOKUP_file());

    if (lookup == NULL) {
        easy_ssl_error(EASY_LOG_ERROR, "X509_STORE_add_lookup() failed");
        return EASY_ERROR;
    }

    if (X509_LOOKUP_load_file(lookup, crl, X509_FILETYPE_PEM) == 0) {
        easy_ssl_error(EASY_LOG_ERROR, "X509_LOOKUP_load_file(\"%s\") failed", crl);
        return EASY_ERROR;
    }

    X509_STORE_set_flags(store, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);

    return EASY_OK;
}

static void easy_ssl_client_handshake_handler(easy_connection_t *c)
{
    SSL_SESSION             *old_ssl_session, *ssl_session;

    if (c->sc->handshaked) {
        ev_set_cb(&c->read_watcher, easy_connection_on_readable);
        ev_set_cb(&c->write_watcher, easy_connection_on_writable);

        if (c->sc->session_reuse) {
            ssl_session = SSL_get1_session(c->sc->connection);

            if (ssl_session == NULL) {
                return;
            }

            old_ssl_session = c->client->ssl_session;
            c->client->ssl_session = ssl_session;

            if (old_ssl_session) {
                SSL_SESSION_free(old_ssl_session);
            }
        }
    }
}

static int easy_ssl_pass_phrase_cb(char *buf, int size, int rwflag, void *conf)
{
    easy_ssl_pass_phrase_dialog_t  *dialog;
    int                             len = -1;

    dialog = (easy_ssl_pass_phrase_dialog_t *)conf;
    buf[0] = '\0';

    if (dialog->type == NULL || strncmp(dialog->type, "builtin", 7) == 0) {
        easy_error_log("Server %s", dialog->server_name);

        while ((len = strlen(buf)) == 0) {
            fprintf(stderr, "Enter pass phrase:");

            if ((len = EVP_read_pw_string(buf, size, "", 0)) != 0) {
                return -1;
            }
        }
    } else if (strncmp(dialog->type, "exec:", 5) == 0) {
        FILE                    *fp;
        char                    *p, cmd[256];

        snprintf(cmd, 256, "%s '%s'", dialog->type + 5, dialog->server_name);

        if ((fp = popen(cmd, "r")) == NULL)
            return -1;

        if (fgets(buf, size, fp)) {
            p = buf + strlen(buf);

            while(p > buf && *(p - 1) == '\n') p --;

            *p = '\0';
            len = p - buf;
        }

        pclose(fp);
    } else if (strncmp(dialog->type, "text:", 5) == 0) {
        len = lnprintf(buf, size, "%s", dialog->type + 5);
    }

    return len;
}

int easy_ssl_client_authenticate(easy_ssl_t *ssl, SSL *conn, const void *host, int len)
{
    uint64_t                key = easy_hash_code(host, len, 3);
    easy_ssl_ctx_server_t   *cs = (easy_ssl_ctx_server_t *)easy_hash_find_ex(ssl->server_map, key, easy_ssl_ctx_server_cmp, host);

    if (cs && cs->ss->conf.verify) {
        long                    rc = SSL_get_verify_result(conn);

        if (rc != X509_V_OK) {
            easy_error_log("client SSL certificate verify error: (%l:%s)",
                           rc, X509_verify_cert_error_string(rc));
            return 0;
        }

        if (cs->ss->conf.verify == 1) {
            X509                    *cert = SSL_get_peer_certificate(conn);

            if (cert == NULL) {
                easy_error_log("client sent no required SSL certificate");
                return 0;
            }

            X509_free(cert);
        }
    }

    return 1;
}
