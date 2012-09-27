AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/statserver/include \
			 -I${top_srcdir}/src/dataserver \
	     	@BOOST_CPPFLAGS@

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-lz -lrt -lpthread -ldl  ${GCOV_LIB}

include_HEADERS= packet_factory.hpp base_packet.hpp \
		 duplicate_packet.hpp \
		 items_packet.hpp \
		 query_info_packet.hpp \
		 put_packet.hpp get_packet.hpp \
		 response_return_packet.hpp

noinst_LIBRARIES=libtair_packets.a
libtair_packets_a_SOURCES=packet_factory.cpp	\
        base_packet.hpp  \
        conf_heartbeat_packet.hpp  \
        data_server_ctrl_packet.hpp  \
        stat_cmd_packet.hpp \
        flow_control_packet.hpp \
        flow_view.hpp \
        dump_bucket_packet.hpp  \
        dump_packet.hpp  \
        duplicate_packet.hpp  \
        get_group_packet.hpp  \
        get_migrate_machine.hpp  \
        get_packet.hpp  \
        get_server_table_packet.hpp  \
        group_names_packet.hpp  \
        heartbeat_packet.hpp  \
        inc_dec_packet.hpp  \
        items_packet.hpp  \
        migrate_finish_packet.hpp  \
        mupdate_packet.hpp  \
        packet_factory.hpp  \
        ping_packet.hpp  \
        invalid_packet.hpp \
        hide_packet.hpp \
        hide_by_proxy_packet.hpp \
        get_hidden_packet.hpp \
        sync_packet.hpp \
        put_packet.hpp  \
        expire_packet.hpp  \
        query_info_packet.hpp  \
        remove_area_packet.hpp  \
        remove_packet.hpp  \
        response_return_packet.hpp  \
        server_hash_table_packet.hpp  \
        set_master_packet.hpp \
        response_mreturn_packet.hpp \
        prefix_incdec_packet.hpp \
        prefix_puts_packet.hpp \
        prefix_removes_packet.hpp \
        prefix_gets_packet.hpp \
        prefix_hides_by_proxy_packet.hpp \
        prefix_hides_packet.hpp \
        prefix_invalids_packet.hpp \
        prefix_get_hiddens_packet.hpp \
        lock_packet.hpp \
        op_cmd_packet.hpp \
        packet_streamer.hpp \
        packet_streamer.cpp get_range_packet.hpp \
				debug_support_packet.hpp
