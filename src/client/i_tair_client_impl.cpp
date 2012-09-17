/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair client api interface
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "i_tair_client_impl.hpp"

namespace tair
{
  std::map<int, std::string> init_errmsg()
  {
    std::map<int, std::string> temp;
    temp[TAIR_RETURN_SUCCESS]                 = "success";
    temp[TAIR_RETURN_FAILED]          = "general failed";
    temp[TAIR_RETURN_NOT_INIT]          = "not init";
    temp[TAIR_RETURN_DATA_NOT_EXIST]  = "data not exists";
    temp[TAIR_RETURN_VERSION_ERROR]           = "version error";
    temp[TAIR_RETURN_TYPE_NOT_MATCH]  = "data type not match";
    temp[TAIR_RETURN_ITEM_EMPTY]              = "item is empty";
    temp[TAIR_RETURN_SERIALIZE_ERROR] = "serialize failed";
    temp[TAIR_RETURN_OUT_OF_RANGE]            = "item's index out of range";
    temp[TAIR_RETURN_ITEMSIZE_ERROR]  = "key or value too large";
    temp[TAIR_RETURN_SEND_FAILED]             = "send packet error";
    temp[TAIR_RETURN_TIMEOUT]         = "timeout";
    temp[TAIR_RETURN_SERVER_CAN_NOT_WORK]   = "server can not work";
    temp[TAIR_RETURN_WRITE_NOT_ON_MASTER]   = "write not on master";
    temp[TAIR_RETURN_DUPLICATE_BUSY]          = "duplicate busy";
    temp[TAIR_RETURN_MIGRATE_BUSY]    = "migrate busy";
    temp[TAIR_RETURN_PARTIAL_SUCCESS] = "partial success";
    temp[TAIR_RETURN_DATA_EXPIRED]            = "expired";
    temp[TAIR_RETURN_PLUGIN_ERROR]            = "plugin error";
    temp[TAIR_RETURN_PROXYED]                 = "porxied";
    temp[TAIR_RETURN_INVALID_ARGUMENT]        = "invalid argument";
    temp[TAIR_RETURN_CANNOT_OVERRIDE] = "cann't override old value.please check and remove it first.";
    temp[TAIR_RETURN_DEC_BOUNDS] = "can't dec to negative when allow_count_negative=0";
    temp[TAIR_RETURN_DEC_ZERO] = "can't dec zero number when allow_count_negative=0";
    temp[TAIR_RETURN_DEC_NOTFOUND] = "dec but not found when allow_count_negative=0";
    temp[TAIR_RETURN_LOCK_EXIST]                 = "lock exist";
    temp[TAIR_RETURN_LOCK_NOT_EXIST]                 = "lock not exist";
    temp[TAIR_RETURN_HIDDEN] = "item is hidden";
    temp[TAIR_RETURN_SHOULD_PROXY] = "the key should be proxy";
    //temp[TAIR_RETURN_NO_INVALID_SERVER]       = "invlaid but not found invalid server";
    return temp;
  }

  const std::map<int, std::string> i_tair_client_impl::errmsg_ = init_errmsg();
  i_tair_client_impl::i_tair_client_impl() {}
  i_tair_client_impl::~i_tair_client_impl() {}

}
