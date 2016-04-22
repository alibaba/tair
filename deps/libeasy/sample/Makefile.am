AM_CFLAGS= -I${top_srcdir}/src/include     \
        -I${top_srcdir}/src/util           \
        -I${top_srcdir}/src/thread         \
        -I${top_srcdir}/src/io             \
        -I${top_srcdir}/src/packet/http

EXTRA_DIST = echo_packet.h echo_packet_tbnet.h Makefile.sample http_status.init ssl.conf ca.key ca.crt
AM_LDFLAGS=-lpthread -lm -lrt -lcrypto

LDADD=${PRESET_LDADD}
noinst_PROGRAMS=echo_server echo_client     \
    echo_server_tbnet echo_client_tbnet     \
    http_server http_client                 \
    uthread_test echo_client_uthread        \
    echo_client_simple http_status          \
    kfc_test easy_kfc_load easy_kfc_tool echo_server_async \
    https_server https_client dns_server proxy_server

echo_server_SOURCES=echo_server.c
echo_client_SOURCES=echo_client.c
echo_server_tbnet_SOURCES=echo_server_tbnet.c
echo_client_tbnet_SOURCES=echo_client_tbnet.c
echo_client_simple_SOURCES=echo_client_simple.c
http_server_SOURCES=http_server.c
dns_server_SOURCES=dns_server.c
http_client_SOURCES=http_client.c
uthread_test_SOURCES=uthread_test.c
echo_client_uthread_SOURCES=echo_client_uthread.c
#press_easy_mem_pool_SOURCES=press_easy_mem_pool.c
http_status_SOURCES=http_status.c
kfc_test_SOURCES=kfc_test.c
easy_kfc_load_SOURCES=easy_kfc_load.cpp
easy_kfc_tool_SOURCES=easy_kfc_tool.cpp
echo_server_async_SOURCES=echo_server_async.c
https_server_SOURCES=https_server.c
https_client_SOURCES=https_client.c

am__DEPENDENCIES_1=${PRESET_LDADD}
${PRESET_LDADD}:
	rm -f ${top_srcdir}/src/libeasy.la
	cd ${top_srcdir}/src && make

echopacket.pb.cpp: echopacket.proto
	protoc --cpp_out . $<
	rename .cc .cpp *.cc

