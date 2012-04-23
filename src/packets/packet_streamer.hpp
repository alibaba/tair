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

#ifndef TAIR_PACKET_STREAMER_H
#define TAIR_PACKET_STREAMER_H

#include <tbnet.h>

namespace tair {

class tair_packet_streamer : public tbnet::IPacketStreamer {

public:
    /*
     * 构造函数
     */
    tair_packet_streamer();

    /*
     * 构造函数
     */
    tair_packet_streamer(tbnet::IPacketFactory *factory);

    /*
     * 析造函数
     */
    ~tair_packet_streamer();

    /**
     * 设置IPacketFactory
     */
    void setPacketFactory(tbnet::IPacketFactory *factory);

    /*
     * 得到包头信息
     *
     * @param input  源buffer
     * @param header 结果header
     * @return 是否成功
     */
    bool getPacketInfo(tbnet::DataBuffer *input, tbnet::PacketHeader *header, bool *broken);

    /*
     * 对包的解码
     *
     * @param input
     * @param header
     * @return 解码后的数据包
     */
    tbnet::Packet *decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header);

    /*
     * 对Packet的组装
     *
     * @param packet 数据包
     * @param output 组装后的数据流
     * @return 是否成功
     */
    bool encode(tbnet::Packet *packet, tbnet::DataBuffer *output);

    /*
     * 设置packet的flag
     */
    void setPacketFlag(int flag);

public:
    int packet_flag;
};

}

#endif /*TAIR_PACKET_STREAMER_H*/
