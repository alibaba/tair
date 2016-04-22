AM_CPPFLAGS=-I${top_srcdir}/test/

LDADD=${PRESET_LDADD} -lpthread -lm -lcrypto
noinst_PROGRAMS = io_test
TESTS = $(noinst_PROGRAMS)
io_test_SOURCES = io_test.c \
    easy_io_test.c          \
    easy_baseth_pool_test.c \
    easy_client_test.c      \
    easy_connection_test.c  \
    easy_log_test.c         \
    easy_request_test.c     \
    easy_event_test.c       \
    easy_file_test.c

am__DEPENDENCIES_1=${PRESET_LDADD}
${PRESET_LDADD}:
	rm -f ${top_srcdir}/src/libeasy.la
	cd ${top_srcdir}/src && make


