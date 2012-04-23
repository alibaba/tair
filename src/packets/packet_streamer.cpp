/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *
 */

#include "define.hpp"
#include "packet_streamer.hpp"

namespace tair {

/*
 * 构造函数
 */
tair_packet_streamer::tair_packet_streamer() : packet_flag(TAIR_PACKET_FLAG) {}
/*
 * 构造函数
 */
tair_packet_streamer::tair_packet_streamer(tbnet::IPacketFactory *factory)
  : IPacketStreamer(factory), packet_flag(TAIR_PACKET_FLAG) {}

/*
 * 析造函数
 */
tair_packet_streamer::~tair_packet_streamer() {}

/**
 * 设置IPacketFactory
 */
void tair_packet_streamer::setPacketFactory(tbnet::IPacketFactory *factory) {
    _factory = factory;
}

/*
 * 得到包头信息
 *
 * @param input  源buffer
 * @param header 结果header
 * @return 是否成功
 */
bool tair_packet_streamer::getPacketInfo(tbnet::DataBuffer *input, tbnet::PacketHeader *header, bool *broken) {
    if (_existPacketHeader) {
        if (input->getDataLen() < (int)(4 * sizeof(int)))
            return false;
        int flag = input->readInt32();
        header->_chid = input->readInt32();
        header->_pcode = input->readInt32();
        header->_dataLen = input->readInt32();
        if (flag != packet_flag || header->_dataLen < 0 ||
                header->_dataLen > 0x4000000) { // 64M
            TBSYS_LOG(ERROR, "stream error: %x<>%x, dataLen: %d", flag, packet_flag, header->_dataLen);
            *broken = true;
        }
    } else if (input->getDataLen() == 0) {
        return false;
    }
    return true;
}

/*
 * 对包的解码
 *
 * @param input
 * @param header
 * @return 解码后的数据包
 */
tbnet::Packet *tair_packet_streamer::decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
    assert(_factory != NULL);
    tbnet::Packet *packet = _factory->createPacket(header->_pcode);
    if (packet != NULL) {
        if (!packet->decode(input, header)) {
            packet->free();
            packet = NULL;
        }
    } else {
        input->drainData(header->_dataLen);
    }
    return packet;
}

/*
 * 对Packet的组装
 *
 * @param packet 数据包
 * @param output 组装后的数据流
 * @return 是否成功
 */
bool tair_packet_streamer::encode(tbnet::Packet *packet, tbnet::DataBuffer *output) {
    tbnet::PacketHeader *header = packet->getPacketHeader();

    // 为了当encode失败恢复时用
    int oldLen = output->getDataLen();
    // dataLen的位置
    int dataLenOffset = -1;
    int headerSize = 0;

    // 允许存在头信息,写出头信息
    if (_existPacketHeader) {
        output->writeInt32(packet_flag);
        output->writeInt32(header->_chid);
        output->writeInt32(header->_pcode);
        dataLenOffset = output->getDataLen();
        output->writeInt32(0);
        headerSize = 4 * sizeof(int);
    }
    // 写数据
    if (packet->encode(output) == false) {
        TBSYS_LOG(ERROR, "encode error");
        output->stripData(output->getDataLen() - oldLen);
        return false;
    }
    // 计算包长度
    header->_dataLen = output->getDataLen() - oldLen - headerSize;
    // 最终把长度回到buffer中
    if (dataLenOffset >= 0) {
        unsigned char *ptr = (unsigned char *)(output->getData() + dataLenOffset);
        output->fillInt32(ptr, header->_dataLen);
    }

    return true;
}

/*
 * 设置packet的flag
 */
void tair_packet_streamer::setPacketFlag(int flag) {
    packet_flag = flag;
}

}

/////////////
