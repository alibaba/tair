AM_CPPFLAGS= -I${top_srcdir}/deps/libeasy-install/include/easy \
			 -I${top_srcdir}/deps/tbsys-install/include \
			 -I${top_srcdir}/src \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/dataserver

LDADD=${top_srcdir}/deps/tbsys-install/lib/libtbsys.a

AM_LDFLAGS=-lsnappy -lz -lrt -lpthread -ldl  ${GCOV_LIB}
AM_CXXFLAGS=-Werror

include_HEADERS=packet_factory.hpp \
				packet.hpp \
				base_packet.hpp \
				duplicate_packet.hpp \
				query_info_packet.hpp \
				put_packet.hpp get_packet.hpp \
				mc_ops_packet.hpp \
				response_return_packet.hpp

noinst_LIBRARIES=libtair_packets.a
libtair_packets_a_SOURCES=packet_factory.cpp	\
						  packet.hpp \
						  packetqueue.hpp \
						  packetqueue.cpp \
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
						  migrate_finish_packet.hpp  \
						  recovery_finish_packet.hpp  \
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
						  uniq_remove_packet.hpp  \
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
						  mc_ops_packet.hpp \
						  get_range_packet.hpp \
						  retry_all_packet.hpp \
						  inval_stat_packet.hpp \
						  inc_dec_bounded_packet.hpp \
						  simple_prefix_get_packet.hpp \
						  prefix_incdec_bounded_packet.hpp \
						  statistics_packet.hpp \
						  rt_packet.hpp \
						  feedback_packet.hpp \
						  admin_config_packet.hpp	\
						  statistics_packet.hpp \
						  get_group_non_down_dataserver_packet.hpp

