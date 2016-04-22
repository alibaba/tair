AM_CPPFLAGS=-I${top_srcdir}/test/

LDADD=${PRESET_LDADD}
AM_LDFLAGS=-lpthread -lm -lcrypto
noinst_PROGRAMS = packet_test
TESTS = $(noinst_PROGRAMS)
packet_test_SOURCES = packet_test.c \
    easy_http_handler_test.c        \
    easy_kfc_handler_test.c

am__DEPENDENCIES_1=${PRESET_LDADD}
${PRESET_LDADD}:
	rm -f ${top_srcdir}/src/libeasy.la
	cd ${top_srcdir}/src && make

