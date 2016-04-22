#ifndef EASY_CONNECTION_H_
#define EASY_CONNECTION_H_

#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * 连接主程序
 */

EASY_CPP_START

typedef struct easy_connection_list_t {
    easy_connection_t       *head;
    easy_connection_t       *tail;
} easy_connection_list_t;

// fuction
easy_listen_t *easy_connection_listen_addr(easy_io_t *eio, easy_addr_t addr, easy_io_handler_pt *handler);
void easy_connection_on_wakeup(struct ev_loop *loop, ev_async *w, int revents);
void easy_connection_on_listen(struct ev_loop *loop, ev_timer *w, int revents);
int easy_connection_write_socket(easy_connection_t *c);
int easy_connection_request_process(easy_request_t *r, easy_io_process_pt *process);
uint64_t easy_connection_get_packet_id(easy_connection_t *c, void *packet, int flag);

int easy_connection_send_session_list(easy_list_t *list);
int easy_connection_session_build(easy_session_t *s);
void easy_connection_wakeup_session(easy_connection_t *c);
void easy_connection_destroy(easy_connection_t *c);
int easy_connection_request_done(easy_request_t *c);
int easy_connection_write_again(easy_connection_t *c);
void easy_connection_on_readable(struct ev_loop *loop, ev_io *w, int revents);
void easy_connection_on_writable(struct ev_loop *loop, ev_io *w, int revents);
void easy_connection_reuseport(easy_io_t *eio, easy_listen_t *l, int idx);
void easy_connection_on_accept(struct ev_loop *loop, ev_io *w, int revents);
void easy_connection_on_udpread(struct ev_loop *loop, ev_io *w, int revents);

// user timer
int easy_connection_user_timer_start(easy_connection_t *c, easy_timer_pt *handler, double after, double repeat);
int easy_connection_user_timer_stop(easy_connection_t *c);

easy_connection_t *easy_connection_new();
void easy_connection_on_timeout_conn(struct ev_loop *loop, ev_timer *w, int revents);
void easy_connection_evio_start(easy_connection_t *c);

EASY_CPP_END

#endif

