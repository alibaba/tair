AM_CPPFLAGS=-I${top_srcdir}/test/

LDADD=${PRESET_LDADD}
AM_LDFLAGS = -lpthread
noinst_PROGRAMS = thread_test
TESTS = $(noinst_PROGRAMS)
thread_test_SOURCES = thread_test.c \
	easy_uthread_test.c

am__DEPENDENCIES_1=${PRESET_LDADD}
${PRESET_LDADD}:
	rm -f ${top_srcdir}/src/libeasy.la
	cd ${top_srcdir}/src && make
