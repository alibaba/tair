/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#include "easy_helper.hpp"
#include "packet_factory.hpp"
#include "ping_packet.hpp"
#include "base_packet.hpp"
#include "flow_control_packet.hpp"
#include "tair_client_api_impl.hpp"
#include <sys/epoll.h>

namespace tair {
easy_io_t *easy_helper::eio = NULL;
easy_io_handler_pt *easy_helper::handler = NULL;

int easy_helper::tair_on_connect_cb(easy_connection_t *c) {
    c->idle_time = 15 * 60 * 1000; //~ disconnect when idle for 15min
    c->life_idle = 1;
    log_info("connection established: %s", easy_connection_to_str(c).c_str());
    return EASY_OK;
}

int easy_helper::tair_on_disconnect_cb(easy_connection_t *c) {
    log_info("disconnect: %s", easy_connection_to_str(c).c_str());
    return EASY_OK;
}

int easy_helper::tair_on_idle_cb(easy_connection_t *c) {
    log_info("disconnect for %d seconds idleness: %s, c->last_time: %lf",
             c->idle_time / 2000, easy_connection_to_str(c).c_str(), c->last_time);
    return EASY_ERROR;
}

int easy_helper::tair_cleanup_cb(easy_request_t *r, void *packet) {
    if (r == NULL && packet == NULL) {
        return EASY_OK;
    }

    if (packet != NULL) {
        delete (base_packet *) packet;
    }
    if (r != NULL) {
        if (r->ipacket != NULL) {
            delete (base_packet *) r->ipacket;
            r->ipacket = NULL;
        }
        if (r->opacket != NULL) {
            Packet *prev = (Packet *) r->opacket;
            while (prev != NULL) {
                Packet *next = prev->_next;
                delete prev;
                prev = next;
            }
            r->opacket = NULL;
        }
    }
    return EASY_OK;
}

void easy_helper::tair_buf_cleanup_cb(easy_buf_t *b, void *arg) {
    easy_pool_destroy((easy_pool_t *) arg);
}

easy_addr_t easy_helper::convert_addr(uint64_t server_id, uint32_t idx) {
    easy_addr_t addr;
    memset(&addr, 0, sizeof(easy_addr_t));
    addr.family = AF_INET;
    addr.port = htons((server_id >> 32) & 0xffff);
    addr.u.addr = (server_id & 0xffffffffUL);
    addr.cidx = idx;
    return addr;
}

uint64_t easy_helper::convert_addr(const easy_addr_t &addr) {
    const struct sockaddr_in *s = (const struct sockaddr_in *) &addr;
    uint64_t id = htons(s->sin_port);
    id <<= 32;
    id |= s->sin_addr.s_addr;
    return id;
}

int easy_helper::alive_processor_cb(easy_request_t *r) {
    int rc = r->ipacket != NULL ? EASY_OK : EASY_ERROR;
    easy_session_destroy(r->ms);
    return rc;
}

bool easy_helper::is_alive(uint64_t server_id, easy_io_t *eio) {
    easy_addr_t addr = convert_addr(server_id);
    addr.cidx = 0;
    return is_alive(addr, eio);
}

bool easy_helper::is_alive(easy_addr_t &addr, easy_io_t *eio) {
#if 1
    //~ using `connect'
    struct sockaddr sa;
    int sfd;
    if ((sfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        log_error("establish socket failed: %s", strerror(errno));
        return false;
    }
    fcntl(sfd, F_SETFL, O_NONBLOCK);
    memset(&sa, 0, sizeof(sa));
    memcpy(&sa, &addr, sizeof(uint64_t));
    bool alive = false;
    errno = 0;
    if (connect(sfd, &sa, sizeof(sa)) == -1) {
        if (errno == EINPROGRESS) {
            alive = wait_connected(sfd, 10/*ms*/);
        }
    } else {
        alive = true;
    }
    if (!alive) {
        char buf[32];
        log_error("connect to '%s' failed: %m",
                  easy_inet_addr_to_str(&addr, buf, sizeof(buf)));
    }
    close(sfd);
    return alive;
#else
    //~ using ping packet
    if (eio == NULL) {
      eio = tair::easy_helper::eio;
    }
    if (eio == NULL) {
      log_error("you must specify a eio");
      return false;
    }
    static easy_io_handler_pt io_handler;
    init_handler(&io_handler, NULL);
    io_handler.process = alive_processor_cb;

    request_ping *packet = new request_ping;
    base_packet *bp = (base_packet*)easy_sync_send(eio, addr, packet, (void*)0L, &io_handler, 100/*ms*/);
    if (bp) {
      delete bp;
      return true;
    } else {
      return false;
    }
#endif
}

bool easy_helper::wait_connected(int fd, int timeout/*ms*/) {
    int ep = epoll_create(1);
    if (ep == -1) {
        log_error("epoll_create failed: %m");
        return false;
    }
    bool ret = false;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    struct epoll_event ev_wait;
    ev.events = (EPOLLOUT | EPOLLERR); //~ we don't expect incoming data
    ev.data.fd = fd;
    if (epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev) == -1) {
        log_error("epoll_ctl::EPOLL_CTL_ADD failed: %m");
        close(ep);
        return false;
    }
    do {
        int n = epoll_wait(ep, &ev_wait, 1, timeout);
        if (n == -1) {
            log_error("epoll_wait failed: %d, %m", fd);
            break;
        }
        if (n == 0) {
            errno = ETIMEDOUT;
            break;
        }
        assert(ev_wait.data.fd == fd);
        if (ev_wait.events & (EPOLLOUT | EPOLLERR)) {
            int err = 0;
            socklen_t len = sizeof(err);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0) {
                ret = true;
            }
            errno = err;
        }
    } while (false);
    epoll_ctl(ep, EPOLL_CTL_DEL, fd, NULL);
    close(ep);
    return ret;
}

int easy_helper::easy_async_send(easy_io_t *eio,
                                 uint64_t server_id,
                                 void *packet,
                                 void *args,
                                 easy_io_handler_pt *handler,
                                 int timeout,
                                 int retry) {

    easy_addr_t addr = convert_addr(server_id);
    return easy_async_send(eio, addr, packet, args, handler, timeout, retry);
}

void easy_helper::init_handler(easy_io_handler_pt *handler, void *user_data) {
    memset(handler, 0, sizeof(*handler));
    handler->encode = packet_factory::encode_cb;
    handler->decode = packet_factory::decode_cb;
    handler->get_packet_id = packet_factory::get_packet_id_cb;
    handler->cleanup = easy_helper::tair_cleanup_cb;
    handler->on_connect = easy_helper::tair_on_connect_cb;
    handler->on_disconnect = easy_helper::tair_on_disconnect_cb;
    handler->on_idle = easy_helper::tair_on_idle_cb;
    /*
     * *NOTE*
     * if you do not wanna die a misable,
     * NERVER convert between pointers to
     * base class and multiple-inherited derived class,
     * through a pointer of void type.
     */
    handler->user_data = user_data;
}

int easy_helper::easy_async_send(easy_io_t *eio,
                                 const easy_addr_t &addr,
                                 void *packet,
                                 void *args,
                                 easy_io_handler_pt *handler,
                                 int timeout,
                                 int retry) {
    if (eio == NULL) {
        eio = tair::easy_helper::eio;
    }
    if (handler == NULL) {
        handler = tair::easy_helper::handler;
    }
    if (eio == NULL || handler == NULL || packet == NULL) {
        log_error("eio, handler, or packet is null");
        return EASY_ERROR;
    }
    //~ establish one session
    easy_session_t *s = easy_session_create(0);
    if (s == NULL) {
        log_error("create easy session failed");
        return EASY_ERROR;
    }
    s->status = EASY_CONNECT_SEND; //~ 5
    s->packet_id = retry;
    s->thread_ptr = handler;
    s->process = (easy_io_process_pt *) handler->process;
    s->cleanup = tair_cleanup_cb;
    easy_session_set_timeout(s, timeout);
    easy_session_set_args(s, args);
    s->r.opacket = packet;

    //~ send
    if (easy_client_dispatch(eio, addr, s) != EASY_OK) {
        s->r.opacket = NULL; //! let the caller handle the `packet'
        easy_session_destroy(s);
        return EASY_ERROR;
    }
    return EASY_OK;
}

void *easy_helper::easy_sync_send(easy_io_t *eio,
                                  uint64_t server_id,
                                  void *packet,
                                  void *args,
                                  easy_io_handler_pt *handler,
                                  int timeout,
                                  int retry) {
    easy_addr_t addr = convert_addr(server_id);
    return easy_sync_send(eio, addr, packet, args, handler, timeout, retry);
}

void *easy_helper::easy_sync_send(easy_io_t *eio,
                                  const easy_addr_t &addr,
                                  void *packet,
                                  void *args,
                                  easy_io_handler_pt *handler,
                                  int timeout,
                                  int retry) {
    if (eio == NULL) {
        eio = tair::easy_helper::eio;
    }
    if (handler == NULL) {
        handler = tair::easy_helper::handler;
    }
    if (eio == NULL || handler == NULL || packet == NULL) {
        log_error("eio, handler, or packet is null");
        return NULL;
    }
    //~ establish one session
    easy_session_t *s = easy_session_create(0);
    if (s == NULL) {
        log_error("create easy session failed");
        return NULL;
    }
    s->status = EASY_CONNECT_SEND;
    s->packet_id = retry;
    s->thread_ptr = handler;
    easy_session_set_timeout(s, timeout);
    easy_session_set_args(s, args);
    s->cleanup = tair_cleanup_cb;
    s->r.opacket = packet;
    s->addr = addr;
    //~ send
    void *resp = easy_client_send(eio, addr, s);
    s->r.ipacket = NULL; //~ hand packet to the caller
    easy_session_destroy(s);
    return resp;
}

void easy_helper::easy_set_log_level(int tbsys_level) {
    switch (tbsys_level) {
        case TBSYS_LOG_LEVEL_ERROR:
#ifdef TBSYS_LOG_LEVEL_USER_ERROR
            case TBSYS_LOG_LEVEL_USER_ERROR:
#endif
            easy_log_level = EASY_LOG_ERROR;
            break;
        case TBSYS_LOG_LEVEL_WARN:
            easy_log_level = EASY_LOG_WARN;
            break;
#ifdef TBSYS_LOG_LEVEL_TRACE
            case TBSYS_LOG_LEVEL_TRACE:
#endif
        case TBSYS_LOG_LEVEL_INFO:
            easy_log_level = EASY_LOG_INFO;
            break;
        case TBSYS_LOG_LEVEL_DEBUG:
            easy_log_level = EASY_LOG_ALL;
            break;
        default:
            if (tbsys_level < TBSYS_LOG_LEVEL_ERROR) {
                easy_log_level = EASY_LOG_ERROR;
            } else {
                easy_log_level = EASY_LOG_ALL;
            }
            break;
    }
}

}
