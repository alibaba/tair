#include "easy_socket.h"
#include "easy_io.h"
#include <easy_inet.h>
#include <easy_string.h>
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>

static int easy_socket_chain_writev(int fd, easy_list_t *l, struct iovec *iovs, int cnt, int *again);
static int easy_socket_sendfile(int fd, easy_file_buf_t *fb, int *again);

/**
 * 打开监听端口
 */
int easy_socket_listen(int udp, easy_addr_t *address, int *flags)
{
    int                     v, fd = -1;

    if ((fd = socket(address->family, (udp ? SOCK_DGRAM : SOCK_STREAM), 0)) < 0) {
        easy_trace_log("create socket error.\n");
        goto error_exit;
    }

    if (udp == 0) {
        easy_socket_non_blocking(fd);

        if ((*flags & EASY_FLAGS_DEFERACCEPT)) {
            easy_socket_set_tcpopt(fd, TCP_DEFER_ACCEPT, 1);
            easy_socket_set_tcpopt(fd, TCP_SYNCNT, 2);
        }
    }

    // set reuseport, reuseaddr
    v = (flags && (*flags & EASY_FLAGS_SREUSEPORT)) ? 1 : 0;

    if (v) {
        easy_socket_set_opt(fd, SO_REUSEPORT, 1);
    } else {
        if (easy_socket_set_opt(fd, SO_REUSEPORT, 1) == 0) {
            //easy_socket_set_opt(fd, SO_REUSEPORT, 0);

            if (address->family == AF_INET) *flags = EASY_FLAGS_REUSEPORT;
        }

        easy_socket_set_opt(fd, SO_REUSEADDR, 1);
    }

    if (address->family == AF_INET6) {
        struct sockaddr_in6     addr;
        memset(&addr, 0, sizeof(struct sockaddr_in6));
        addr.sin6_family = AF_INET6;
        addr.sin6_port = address->port;
        memcpy(&addr.sin6_addr, address->u.addr6, sizeof(address->u.addr6));

        if (bind(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in6)) < 0) {
            easy_trace_log("bind socket error: %d\n", errno);
            goto error_exit;
        }

    } else {
        struct sockaddr_in      addr;
        socklen_t               len = sizeof(addr);
        memset(&addr, 0, sizeof(struct sockaddr_in));
        memcpy(&addr, address, sizeof(uint64_t));

        if (bind(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0) {
            easy_trace_log("bind socket error: %d\n", errno);
            goto error_exit;
        }

        if (address->port == 0 && getsockname(fd, (struct sockaddr *)&addr, &len) == 0) {
            memcpy(address, &addr, sizeof(uint64_t));
        }
    }

    if (udp == 0 && listen(fd, 10240) < 0) {
        easy_trace_log("listen error. %d\n", errno);
        goto error_exit;
    }

    return fd;

error_exit:

    if (fd >= 0)
        close(fd);

    return -1;
}

/**
 * 从socket读入数据到buf中
 */
int easy_socket_read(easy_connection_t *c, char *buf, int size, int *pending)
{
    int                     n;

    do {
        n = recv(c->fd, buf, size, 0);
    } while(n == -1 && errno == EINTR);

    if (n < 0) {
        n = ((errno == EAGAIN) ? EASY_AGAIN : EASY_ERROR);
    }

    return n;
}

/**
 * 把buf_chain_t上的内容通过writev写到socket上
 */
#define EASY_SOCKET_RET_CHECK(ret, size, again) if (ret<0) return ret; else size += ret; if (again) return size;
int easy_socket_write(easy_connection_t *c, easy_list_t *l)
{
    return easy_socket_tcpwrite(c->fd, l);
}

int easy_socket_tcpwrite(int fd, easy_list_t *l)
{
    easy_buf_t              *b, *b1;
    easy_file_buf_t         *fb;
    struct          iovec   iovs[EASY_IOV_MAX];
    int                     sended, size, cnt, ret, wbyte, again;

    wbyte = cnt = sended = again = 0;

    // foreach
    easy_list_for_each_entry_safe(b, b1, l, node) {
        // sendfile
        if ((b->flags & EASY_BUF_FILE)) {
            // 先writev出去
            if (cnt > 0) {
                ret = easy_socket_chain_writev(fd, l, iovs, cnt, &again);
                EASY_SOCKET_RET_CHECK(ret, wbyte, again);
                cnt = 0;
            }

            fb = (easy_file_buf_t *)b;
            sended += fb->count;
            ret = easy_socket_sendfile(fd, fb, &again);
            EASY_SOCKET_RET_CHECK(ret, wbyte, again);
        } else {
            size = b->last - b->pos;
            iovs[cnt].iov_base = b->pos;
            iovs[cnt].iov_len = size;
            cnt ++;
            sended += size;
        }

        // 跳出
        if (cnt >= EASY_IOV_MAX || sended >= EASY_IOV_SIZE)
            break;
    }

    // writev
    if (cnt > 0) {
        ret = easy_socket_chain_writev(fd, l, iovs, cnt, &again);
        EASY_SOCKET_RET_CHECK(ret, wbyte, again);
    }

    return wbyte;
}

/**
 * writev
 */
static int easy_socket_chain_writev(int fd, easy_list_t *l, struct iovec *iovs, int cnt, int *again)
{
    int                     ret, sended, size;
    easy_buf_t              *b, *b1;

    do {
        if (cnt == 1) {
            ret = send(fd, iovs[0].iov_base, iovs[0].iov_len, 0);
        } else {
            ret = writev(fd, iovs, cnt);
        }
    } while(ret == -1 && errno == EINTR);

    // 结果处理
    if (ret >= 0) {
        sended = ret;

        easy_list_for_each_entry_safe(b, b1, l, node) {
            size = b->last - b->pos;

            if (easy_log_level >= EASY_LOG_TRACE) {
                char                    btmp[64];
                easy_trace_log("fd: %d write: %d,%d => %s", fd, size, sended, easy_string_tohex(b->pos, size, btmp, 64));
            }

            b->pos += sended;
            sended -= size;

            if (sended >= 0) {
                cnt --;
                easy_buf_destroy(b);
            }

            if (sended <= 0)
                break;
        }
        *again = (cnt > 0);
    } else {
        ret = ((errno == EAGAIN) ? EASY_AGAIN : EASY_ERROR);
    }

    return ret;
}

/**
 * sendfile
 */
static int easy_socket_sendfile(int fd, easy_file_buf_t *fb, int *again)
{
    int                     ret;

    do {
        ret = sendfile(fd, fb->fd, (off_t *)&fb->offset, fb->count);
    } while(ret == -1 && errno == EINTR);

    // 结果处理
    if (ret >= 0) {
        easy_debug_log("sendfile: %d, fd: %d\n", ret, fd);

        if (ret < fb->count) {
            fb->count -= ret;
            *again = 1;
        } else {
            easy_buf_destroy((easy_buf_t *)fb);
        }
    } else {
        ret = ((errno == EAGAIN) ? EASY_AGAIN : EASY_ERROR);
    }

    return ret;
}

// 非阻塞
int easy_socket_non_blocking(int fd)
{
    int                     flags = 1;
    return ioctl(fd, FIONBIO, &flags);
}

// TCP
int easy_socket_set_tcpopt(int fd, int option, int value)
{
    return setsockopt(fd, IPPROTO_TCP, option, (const void *) &value, sizeof(value));
}

// SOCKET
int easy_socket_set_opt(int fd, int option, int value)
{
    return setsockopt(fd, SOL_SOCKET, option, (void *)&value, sizeof(value));
}

// check ipv6
int easy_socket_support_ipv6()
{
    int                     fd = socket(AF_INET6, SOCK_STREAM, 0);

    if (fd == -1)
        return 0;

    close(fd);
    return 1;
}

// udp send
int easy_socket_usend(easy_connection_t *c, easy_list_t *l)
{
    struct sockaddr         addr;
    easy_inet_etoa(&c->addr, &addr);
    return easy_socket_udpwrite(c->fd, &addr, l);
}

int easy_socket_udpwrite(int fd, struct sockaddr *addr, easy_list_t *l)
{
    easy_buf_t              *b, *b1;
    struct msghdr           msg;
    int                     cnt = 0, ret = 0;
    struct          iovec   iovs[EASY_IOV_MAX];
    socklen_t               addr_len = sizeof(struct sockaddr);

    // foreach
    easy_list_for_each_entry(b, l, node) {
        iovs[cnt].iov_base = b->pos;
        iovs[cnt].iov_len = b->last - b->pos;

        if (++ cnt >= EASY_IOV_MAX) break;
    }

    if (cnt > 1) {
        memset(&msg, 0, sizeof(msg));
        msg.msg_name = (struct sockaddr *)addr;
        msg.msg_namelen = addr_len;
        msg.msg_iov = (struct iovec *)iovs;
        msg.msg_iovlen = cnt;

        ret = sendmsg(fd, &msg, 0);
    } else if (cnt == 1) {
        ret = sendto(fd, iovs[0].iov_base, iovs[0].iov_len, 0, (struct sockaddr *)addr, addr_len);
    }

    easy_list_for_each_entry_safe(b, b1, l, node) {
        easy_buf_destroy(b);

        if (-- cnt <= 0) break;
    }
    return ret;
}
