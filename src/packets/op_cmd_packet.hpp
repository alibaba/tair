/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * put packet
 *
 * Version: $Id: op_cmd_packet.hpp 671 2012-03-28 01:54:22Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_OP_CMD_PACKET_H
#define TAIR_PACKET_OP_CMD_PACKET_H

#include "base_packet.hpp"

namespace tair
{
  class request_op_cmd : public base_packet
  {
  public:
    request_op_cmd()
    {
      setPCode(TAIR_REQ_OP_CMD_PACKET);
      cmd = TAIR_SERVER_CMD_NONE;
    }

    virtual ~request_op_cmd()
    {
    }

    bool encode(tbnet::DataBuffer *output)
    {
      output->writeInt32(cmd);
      output->writeInt32(params.size());
      for (std::vector<std::string>::iterator it = params.begin(); it != params.end(); ++it) {
        output->writeString(*it);
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      cmd = static_cast<ServerCmdType>(input->readInt32());
      int param_count = input->readInt32();
      for (int i = 0; i < param_count; ++i) {
        char* tmp = NULL;
        int len = 0;
        // DataBuffer's readString() len is mock.
        input->readString(tmp, len);
        params.push_back(tmp);
        delete tmp;
      }
      return true;
    }

    void add_param(const char* param)
    {
      if (param != NULL) {
        params.push_back(param);
      }
    }

    ServerCmdType cmd;
    std::vector<std::string> params;
  };

  class response_op_cmd : public base_packet {
  public:
    response_op_cmd() {
      setPCode(TAIR_RESP_OP_CMD_PACKET);
      code = 0;
    }

    bool encode(tbnet::DataBuffer *output) {
      output->writeInt32(code);
      output->writeInt32(infos.size());
      for (size_t i = 0; i < infos.size(); ++i) {
        output->writeString(infos[i]);
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      code = input->readInt32();
      int ninfo = input->readInt32();
      if (ninfo < 0) {
        return false;
      }
      infos.reserve(ninfo);
      for (int i = 0; i < ninfo; ++i) {
        char *tmp = NULL;
        input->readString(tmp, 0);
        infos.push_back(tmp);
        ::free(tmp);
      }
      return true;
    }

    int code;
    std::vector<std::string> infos;
  };
}

#endif
