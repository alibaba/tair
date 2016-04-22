CURRENT=3
AGE=0
REVISION=1
VERSION_LOADFLAGS=-version-info ${CURRENT}:${REVISION}:${AGE}

AM_CPPFLAGS=-I${top_srcdir}/src \
	    -I${top_srcdir}/deps/libeasy-install/include/easy \
	    -I${top_srcdir}/deps/tbsys-install/include \
	    -I${top_srcdir}/src/storage/mdb \
	    -I${top_srcdir}/src/common \
	    -I${top_srcdir}/src/storage/ \
	    -I${top_srcdir}/src/packets \
	    -I${top_srcdir}/src/dataserver \
	    -I${top_srcdir}/src/plugin

AM_CXXFLAGS=-Werror -fPIC
AM_CFLAGS=-Werror -fPIC
AM_LDFLAGS=-lcrypto -lz

multi_cluster_impl_source=tair_multi_cluster_client_impl.hpp \
			  tair_multi_cluster_client_impl.cpp \
			  cluster_handler_manager.hpp \
			  cluster_handler_manager.cpp \
			  cluster_info_updater.hpp \
			  cluster_info_updater.cpp

# daimond
if WITH_DIAMOND
mc_impl_source=diamond_manager.hpp \
	       diamond_manager.cpp \
	       cluster_controller.hpp \
	       cluster_controller.cpp \
	       tair_mc_client_api.hpp  \
	       tair_mc_client_api.cpp  \
	       ${top_srcdir}/src/common/cJSON.c

mc_impl_lib=libtairmcclientapi.la
mc_impl_header=tair_mc_client_api.hpp \
	       diamond_manager.hpp \
	       cluster_controller.hpp
endif

api_source_list=tair_client_api.cpp \
		i_tair_client_impl.hpp \
		i_tair_client_impl.cpp \
		tair_client_api_impl.cpp \
		${top_srcdir}/src/packets/packet_factory.cpp \
		${top_srcdir}/src/common/easy_helper.cpp \
		${top_srcdir}/src/common/data_entry.cpp \
		${top_srcdir}/src/common/item_data_info.cpp \
		${top_srcdir}/src/common/timed_collections.hpp \
		${top_srcdir}/src/common/timed_collections.cpp \
		${top_srcdir}/src/common/record.hpp \
		${top_srcdir}/src/common/record.cpp \
		${top_srcdir}/src/common/stat_define.cpp \
		${top_srcdir}/src/common/util.cpp \
		tair_client_api.hpp \
		tair_client_api_impl.hpp \
		local_cache.hpp  \
		local_cache.h \
		flow_admin.cpp \
		flow_admin.h \
		string_local_cache.h \
		string_local_cache.cpp \
		data_entry_local_cache.h \
		data_entry_local_cache.cpp \
		tair_statistics.h \
		tair_statistics.cpp \
		tair_client.hpp \
		${multi_cluster_impl_source}

EASY_LIB= -L${top_srcdir}/deps/libeasy-install/lib64 -leasy
COMMON_LOADFLAGS=-lpthread -lssl -lz -lrt ${GCOV_LIB} ${COMPRESS_LDFLAGS} -lcrypto

lib_LTLIBRARIES=libtairclientapi.la libtairclientapi_impl.la ${mc_impl_lib} ${mc_impl_c_lib}

noinst_LTLIBRARIES=libtairclientapi_noinst.la
libtairclientapi_noinst_la_SOURCES=${api_source_list}

libtairclientapi_la_SOURCES=${api_source_list}
libtairclientapi_la_LDFLAGS=${VERSION_LOADFLAGS}
libtairclientapi_la_LIBADD=${EASY_LIB} ${COMMON_LOADFLAGS}

libtairclientapi_impl_la_SOURCES=${api_source_list}
libtairclientapi_impl_la_LDFLAGS=${VERSION_LOADFLAGS}
libtairclientapi_impl_la_LIBADD=${EASY_LIB} ${COMMON_LOADFLAGS}

if WITH_DIAMOND
libtairmcclientapi_la_SOURCES=${api_source_list} ${mc_impl_source}
libtairmcclientapi_la_LDFLAGS=${VERSION_LOADFLAGS}
libtairmcclientapi_la_LIBADD=${EASY_LIB} ${COMMON_LOADFLAGS}
endif

include_HEADERS=tair_client_api.hpp \
				local_cache.h \
				local_cache.hpp \
				data_entry_local_cache.h \
				tair_statistics.h \
				${mc_impl_header} \
				${mc_impl_c_header}

########################### bin program ##########################################################
if WITH_READLINE
READLINE_LOADFLAGS= -lreadline -ltermcap
endif

BIN_LDADD= ${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
		   ${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
		   $(top_builddir)/src/common/libsnappy.a

sbin_PROGRAMS=tairclient keycleantool manipulate_ds_to_cs

tairclient_SOURCES=tair_client.cpp
tairclient_LDADD= libtairclientapi_noinst.la ${READLINE_LOADFLAGS} ${BIN_LDADD}
tairclient_LDFLAGS= -static ${COMMON_LOADFLAGS}

keycleantool_SOURCES=tair_key_clean.cpp del_job.hpp nonblock_queue.hpp clean_worker.hpp config_loader.hpp
keycleantool_LDADD= libtairclientapi_noinst.la ${BIN_LDADD}
keycleantool_LDFLAGS= -static ${COMMON_LOADFLAGS}

manipulate_ds_to_cs_SOURCES=manipulate_ds_to_cs.cpp
manipulate_ds_to_cs_LDADD= libtairclientapi_noinst.la ${BIN_LDADD}
manipulate_ds_to_cs_LDFLAGS= -static ${COMMON_LOADFLAGS}

