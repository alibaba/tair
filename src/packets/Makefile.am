AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/dataserver \
	     	@BOOST_CPPFLAGS@

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-lz -lrt -lpthread -ldl  ${GCOV_LIB}

include_HEADERS= packet_factory.hpp base_packet.hpp \
		 duplicate_packet.hpp \
		 items_packet.hpp \
		 put_packet.hpp get_packet.hpp \
		 response_return_packet.hpp

noinst_LIBRARIES=libtair_packets.a
libtair_packets_a_SOURCES=packet_factory.cpp	\
			  base_packet.hpp	\
			  conf_heartbeat_packet.hpp	\
			  data_server_ctrl_packet.hpp	\
			  dump_bucket_packet.hpp	\
			  dump_packet.hpp	\
			  duplicate_packet.hpp	\
			  get_group_packet.hpp	\
			  get_migrate_machine.hpp	\
			  get_packet.hpp	\
			  get_server_table_packet.hpp	\
			  group_names_packet.hpp	\
			  heartbeat_packet.hpp	\
			  inc_dec_packet.hpp	\
			  items_packet.hpp	\
			  migrate_finish_packet.hpp	\
			  mupdate_packet.hpp	\
			  packet_factory.hpp	\
			  ping_packet.hpp	\
			  put_packet.hpp	\
			  query_info_packet.hpp	\
			  remove_area_packet.hpp	\
			  remove_packet.hpp	\
			  response_return_packet.hpp	\
			  server_hash_table_packet.hpp	\
			  set_master_packet.hpp
