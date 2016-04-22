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
 *   duolong
 *
 */

#ifndef TBSYS_PUBLIC_DEFINE_H_
#define TBSYS_PUBLIC_DEFINE_H_

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <inttypes.h>
#include <limits.h>

namespace tbsys
{
#define MAX_FILENAME_LEN 256
#define TIME_MAX INT_MAX

/*
 * It's not very common, but very simple
 * If just want to return error
 * or release variables which "clear" in their scopes, this is useful
 */

#define JUST_CONTINUE(ContCond) \
    if (ContCond) \
        continue
#define PRINT_CONTINUE(ContCond, Module, Level, ...) \
    if (ContCond) \
    { \
        Print(Module, Level, __VA_ARGS__); \
        continue; \
    }

#define JUST_BREAK(BreakCond) \
    if (BreakCond) \
        break
#define PRINT_BREAK(BreakCond, Module, Level, ...) \
    if (BreakCond) \
    { \
        Print(Module, Level, __VA_ARGS__); \
        break; \
    }

#define JUST_RETURN(RetCond, Ret) \
    if (RetCond) \
        return Ret

#define PRINT_RETURN(RetCond, Ret, Module, Level, ...) \
    do \
    { \
        if (RetCond) \
        { \
            Print(Module, Level, __VA_ARGS__); \
            return Ret; \
        } \
    } \
    while (0)

#define OUTPUT_RETURN(RetCond,Ret,output,value)\
    do\
    {\
        if (RetCond )\
        {\
            output->writeInt16( value );\
            return Ret;\
        }\
    }\
    while(0);

#define OUTPUTPRINT_RETURN(RetCond,Ret,output,value,Module,Level,...)\
    do\
    {\
         if ( RetCond )\
         {\
             output->writeInt16( value );\
             Print(Module, Level, __VA_ARGS__); \
             return Ret;\
         }\
    }while(0);


#define C_TRY(ExceptionCond) \
    if (ExceptionCond) \
        goto labelException

#define C_PRINT_TRY(ExceptionCond, Module, Level, ...) \
    do \
    { \
        if (ExceptionCond) \
        { \
            Print(Module, Level, __VA_ARGS__); \
            goto labelException; \
        } \
    } \
    while (0)

#define C_CATCH \
    goto labelException; \
labelException:


// return without running clear code
/**
 * 跳转到直接返回错误部分
 * @param ErrorCond: 执行跳转所必需满足的条件
 * @see 
 * @return void
 */
#define RETURN_ERROR(ErrorCond) \
    if (ErrorCond) \
        goto labelClearEnd
// return with running clear code
/**
 * 跳转到需要执行清理处理部分，执行清理完并退出
 * @param ErrorCond: 执行跳转所必需满足的条件
 * @see 
 * @return 
 */
#define GOTO_CLEAR(ErrorCond) \
    if (ErrorCond) \
        goto labelClearBegin
/**
 * 跳转到直接返回错误部分并打印日志
 * @param ErrorCond: 执行跳转所必需满足的条件
 * @param Module: 产生当前错误的模块
 * @param Level: 错误日志的等级
 * @param ...: print格式的输出
 * @see 
 * @return void
 */
#define RETURN_PRINT_ERROR(ErrorCond, Module, Level, ...) \
    do \
    { \
        if (ErrorCond) \
        { \
            Print(Module, Level, __VA_ARGS__); \
            goto labelClearEnd; \
        } \
    } \
    while (0)
/**
 * 跳转到直接返回错误部分并打印日志、执行清理动作
 * @param ErrorCond: 执行跳转所必需满足的条件
 * @param Module: 产生当前错误的模块
 * @param Level: 错误日志的等级
 * @param ...: print格式的输出
 * @see 
 * @return void
 */
#define GOTO_PRINT_CLEAR(ErrorCond, Module, Level, ...) \
    do \
    { \
        if (ErrorCond) \
        { \
            Print(Module, Level, __VA_ARGS__); \
            goto labelClearBegin; \
        } \
    } \
    while (0)
/**
 * 清理动作的开始位置
 * @param void
 * @see 
 * @return void
 */
#define CLEAR_BEGIN \
    { \
        /* tow statments following just for removing warning */ \
        goto labelClearEnd; \
        goto labelClearBegin; \
labelClearBegin:
/**
 * 清理动作的结束位置
 * @param 
 * @see 
 * @return 
 */
#define CLEAR_END \
labelClearEnd:; \
    }

}//end namespace
#endif // PUBLIC_DEFINE_H_
