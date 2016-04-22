AM_CPPFLAGS=-I${top_srcdir}/test/

LDADD=${PRESET_LDADD} -lpthread -lm -lcrypto
noinst_PROGRAMS = util_test
TESTS = $(noinst_PROGRAMS)
util_test_SOURCES = util_test.c \
    easy_pool_test.c            \
    easy_buf_test.c             \
    easy_hash_test.c            \
    easy_string_test.c          \
    easy_inet_test.c            \
    easy_time_test.c            \
    easy_ref_test.c             \
    easy_lock_test.c

am__DEPENDENCIES_1=${PRESET_LDADD}
${PRESET_LDADD}:
	rm -f ${top_srcdir}/src/libeasy.la
	cd ${top_srcdir}/src && make
