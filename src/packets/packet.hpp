/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef TAIR_PACKET_HPP
#define TAIR_PACKET_HPP

#include "define.hpp"

extern "C" {
struct easy_request_t;
};

namespace tair {
class DataBuffer;

class PacketHeader {
public:
    PacketHeader() : _chid(0), _pcode(0), _dataLen(0) {}

    uint32_t _chid;         // channel id
    int _pcode;             // packet code
    int _dataLen;           // packet length excluding packet header
};

class Packet {
public:
    Packet() : _next(NULL), r(NULL), peer_id(0) {}

    Packet(const Packet &p) {
        // Note: if you wnat use copy construction,
        // please ensure knowing each field in Packet clearly
        _packetHeader = p._packetHeader;
        _next = NULL;
        r = p.r;
        peer_id = p.peer_id;
    }

    void setChannelId(uint32_t chid) {
        _packetHeader._chid = chid;
    }

    uint32_t getChannelId() const {
        return _packetHeader._chid;
    }

    void setPCode(int pcode) {
        _packetHeader._pcode = pcode;
    }

    int getPCode() const {
        return _packetHeader._pcode;
    }

    void setDataLen(int datalen) {
        _packetHeader._dataLen = datalen;
    }

    int getDataLen() const {
        return _packetHeader._dataLen;
    }

    PacketHeader *getPacketHeader() {
        return &_packetHeader;
    }

    uint64_t get_peer_id() const {
        return peer_id;
    }

    Packet *getNext() const {
        return _next;
    }

    void setPacketHeader(PacketHeader *header) {
        if (header) {
            memcpy(&_packetHeader, header, sizeof(PacketHeader));
        }
    }

    uint64_t getPeerId() {
        return peer_id;
    }

    virtual bool isRegularPacket() {
        return true;
    }

    void free() {}

    virtual ~Packet() {}

    virtual bool encode(DataBuffer *output) = 0;

    virtual bool decode(DataBuffer *input, PacketHeader *header) = 0;

public:
    PacketHeader _packetHeader;
    Packet *_next;
    easy_request_t *r;
    uint64_t peer_id;
};

class ControlPacket : public Packet {
public:
    enum {
        CMD_BAD_PACKET = 1,
        CMD_TIMEOUT_PACKET,
        CMD_DISCONN_PACKET
    };

    ControlPacket(int c) : _command(c) {}

    bool isRegularPacket() {
        return false;
    }

    int getCommand() {
        return _command;
    }

    bool encode(DataBuffer *output) {
        UNUSED(output);
        return false;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        UNUSED(input);
        UNUSED(header);
        return false;
    }

private:
    int _command;
};
}
#endif
