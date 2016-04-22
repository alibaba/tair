AM_CPPFLAGS=-I${top_srcdir}/test/

LDADD=${PRESET_LDADD}
noinst_PROGRAMS = memory_test
TESTS = $(noinst_PROGRAMS)
memory_test_SOURCES = memory_test.c \
    easy_mem_page_test.c            \
    easy_mem_pool_test.c            \
    easy_mem_slab_test.c       

am__DEPENDENCIES_1=${PRESET_LDADD}
${PRESET_LDADD}:
	rm -f ${top_srcdir}/src/libeasy.la
	cd ${top_srcdir}/src && make
