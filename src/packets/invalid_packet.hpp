/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: invalid_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_INVAL_PACKET_HPP
#define TAIR_PACKET_INVAL_PACKET_HPP
#include "get_packet.hpp"
namespace tair {
    class request_invalid: public request_get {
    public:
        request_invalid(): request_get() {
            group_name[0] = '\0';
            setPCode(TAIR_REQ_INVAL_PACKET);
            is_sync = SYNC_INVALID;
        }

        request_invalid(request_invalid &packet): request_get(packet) {
            set_group_name(packet.group_name);
            setPCode(TAIR_REQ_INVAL_PACKET);
            is_sync = SYNC_INVALID;
        }

        void set_group_name(const char *groupname) {
            if (groupname != NULL) {
                strncpy(group_name, groupname, MAXLEN_GROUP_NAME);
            }
        }

        bool encode(tbnet::DataBuffer *output) {
            if (request_get::encode(output) == false) {
                return false;
            }
            output->writeString(group_name);
            output->writeInt32(is_sync);
            return true;
        }

        bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
            if (request_get::decode(input, header) == false) {
                return false;
            }
            char *p = group_name;
            input->readString(p, MAXLEN_GROUP_NAME);
            group_name[MAXLEN_GROUP_NAME-1] = '\0';
            is_sync = input->readInt32();

            return true;
        }

        void set_sync(uint32_t is_sync) {
          this->is_sync = is_sync;
        }

    public:
        enum { MAXLEN_GROUP_NAME = 64 };
        char group_name[MAXLEN_GROUP_NAME];
        uint32_t is_sync;
    };
}
#endif
