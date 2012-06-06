
AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet  \
	     -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/statserver/include

AM_LDFLAGS=-lz -lrt -lpthread -ldl  ${GCOV_LIB}

noinst_LIBRARIES=libtair_stat.a

libtair_stat_a_SOURCES=stat_manager_mock.cpp flow_controller.cpp flow_controller_impl.h mutex_lock.h

