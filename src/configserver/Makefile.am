AM_CPPFLAGS= -I${top_srcdir}/deps/tbsys-install/include \
			 -I${top_srcdir}/deps/libeasy-install/include/easy \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src

AM_CXXFLAGS=-Werror

AM_LDFLAGS= -rdynamic ${GCOV_LIB} \
      -lssl -lz -lrt -lpthread \
      -ldl -lcrypto

LDADD=${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
      $(top_builddir)/src/plugin/libtair_plugins.a \
      $(top_builddir)/src/packets/libtair_packets.a \
      ${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
      $(top_builddir)/src/common/libtair_common.a

base_src=stat_info.cpp \
		 server_conf_thread.cpp \
		 server_info.cpp \
		 group_info.cpp \
		 conf_server_table_manager.cpp \
		 table_builder.cpp \
		 table_builder1.cpp \
		 table_builder2.cpp \
		 server_info_allocator.cpp \
		 server_info_file_mapper.cpp \
		 conf_server_table_manager.hpp	\
		 group_info.hpp	\
		 server_conf_thread.hpp	\
		 server_info_allocator.hpp	\
		 server_info_file_mapper.hpp	\
		 server_info.hpp	\
		 stat_info.hpp	\
		 table_builder1.hpp	\
		 table_builder2.hpp	\
		 table_builder.hpp	\
		 tair_cfg_svr.hpp

sbin_PROGRAMS=tair_cfg_svr cst_monitor cst_distribution_checker cst_transfer sif_monitor parameter_printer stat_info_test migrate_emulator

parameter_printer_SOURCES=const_parameter_pinter.cpp

tair_cfg_svr_SOURCES=tair_cfg_svr.cpp ${base_src}

cst_monitor_SOURCES=conf_server_table_monitor.cpp ${base_src}

cst_distribution_checker_SOURCES=conf_server_table_checker.cpp ${base_src}

cst_transfer_SOURCES=conf_server_table_transfer.cpp  ${base_src}

sif_monitor_SOURCES=server_info_file_monitor.cpp ${base_src}

stat_info_test_SOURCES=stat_info_test.cpp ${base_src}

migrate_emulator_SOURCES=migrate_emulator.cpp ${base_src}

