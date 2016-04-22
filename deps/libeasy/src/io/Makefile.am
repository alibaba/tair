EXTRA_DIST = ev_vars.h ev_wrap.h ev_epoll.c ev_select.c
include_HEADERS=easy_connection.h       \
	easy_io.h                       \
	easy_file.h                     \
	easy_summary.h                  \
	easy_message.h                  \
	easy_request.h                  \
	easy_socket.h                   \
	easy_client.h                   \
	easy_io_struct.h                \
	easy_baseth_pool.h              \
	easy_log.h                      \
	easy_ssl.h                      \
	easy_event.h                    \
	ev.h

noinst_LTLIBRARIES=libeasy_io.la
libeasy_io_la_SOURCES = easy_socket.c   \
	easy_io.c                       \
	easy_file.c                     \
	ev.c                            \
	easy_connection.c               \
	easy_summary.c                  \
	easy_message.c                  \
	easy_request.c                  \
	easy_baseth_pool.c              \
	easy_log.c                      \
	easy_ssl.c                      \
	easy_event.c                    \
	easy_client.c
