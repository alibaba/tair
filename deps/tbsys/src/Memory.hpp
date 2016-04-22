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

#ifndef TBSYS_MEMORY_HPP_
#define TBSYS_MEMORY_HPP_
#include <new>

namespace tbsys
{
/**
 * 把new关键字转换成函数
 * @see 
 * @return 新创建的Type类的对象(成功)/NULL(失败)
 */
template <typename Type> inline Type *gNew()
{
    Type *Pointer = NULL;
#ifdef _NO_EXCEPTION
    Pointer = new Type;
#else
    try
    {
        Pointer = new Type;
    }
    catch (...)
    {
        Pointer = NULL;
    }
#endif // _NO_EXCEPTION
    return Pointer;
}

/**
 * 把new []关键字转换成函数
 * @param uiItemNum(unsigned): 对象数组的大小
 * @see 
 * @return 包含uiItemNum个类对象的对象数组(成功)/NULL(失败)
 */
template <typename Type> inline Type *gNewA(unsigned uiItemNum)
{
    Type *Pointer = NULL;
#ifdef _NO_EXCEPTION
        Pointer = new Type[uiItemNum];
#else
    try
    {
        Pointer = new Type[uiItemNum];
    }
    catch (...)
    {
        Pointer = NULL;
    }
#endif // _NO_EXCEPTION
    return Pointer;
}

// if not use the default construct, please use this macro, but is can't reference
#ifdef _NO_EXCEPTION
/**
 * 调用有参数的构造函数创建类对象
 * @param Pointer: 输出参数，被赋值为新创建的Class类的对象
 * @param Class: 需创建对象的类名
 * @param ...: 构造函数参数列表
 * @see 
 * @return void
 */
#define ARG_NEW(Pointer, Class, ...) \
    do \
    { \
        Pointer = new Class(__VA_ARGS__); \
    } \
    while (0)
#else
#define ARG_NEW(Pointer, Class, ...) \
    do \
    { \
        try \
        { \
            Pointer = new Class(__VA_ARGS__); \
        } \
        catch (...) \
        { \
            Pointer = NULL; \
        } \
    } \
    while (0)
#endif

/**
 * 把placement new转换成函数
 * @param p(void *): 构造对象的内存地址
 * @see 
 * @return Type类型对象(成功)/NULL(失败)
 */
template <typename Type> inline Type *gConstruct(void *p)
{
    Type *Pointer = NULL;
#ifdef _NO_EXCEPTION
    Pointer = new (p) Type;
#else
    try
    {
        Pointer = new (p) Type;
    }
    catch (...)
    {
        Pointer = NULL;
    }
#endif // _NO_EXCEPTION
    return Pointer;
}

/*
 * if not use the default construct, please use this macro, but is can't reference
 */
#ifdef _NO_EXCEPTION
/**
 * 封装构造函数的异常，并提供构造参数
 * @param Pointer: 输出参数，被赋值为新创建的Class类的对象
 * @param Class: 需创建对象的类名
 * @param Memory: 构造对象的内存地址
 * @param ...: 构造对象的参数列表
 * @see 
 * @return 
 */
#define CONSTRUCT(Pointer, Class, Memory, ...) \
    do \
    { \
        Pointer = new (Memory) Class(__VA_ARGS__); \
    } \
    while (0)
#else
#define CONSTRUCT(Pointer, Class, Memory, ...) \
    do \
    { \
        try \
        { \
            Pointer = new (Memory) Class(__VA_ARGS__); \
        } \
        catch (...) \
        { \
            Pointer = NULL; \
        } \
    } \
    while (0)
#endif // _NO_EXCEPTION

#ifdef _NO_EXCEPTION
/**
 * 调用默认构造函数构造对象，封装异常
 * @param Pointer: 输出参数，被赋值为新创建的Class类的对象
 * @param Class: 需创建对象的类名
 * @see 
 * @return 
 */
#define FRIEND_NEW(Pointer, Class) \
    do \
    { \
        Pointer = new Class; \
    } \
    while (0)
#else
#define FRIEND_NEW(Pointer, Class) \
    do \
    { \
        try \
        { \
            Pointer = new Class; \
        } \
        catch (...) \
        { \
            Pointer = NULL; \
        } \
    } \
    while (0)
#endif

#ifdef _NO_EXCEPTION
/**
 * 调用默认构造函数创建对象数组，封装异常
 * @param Pointer: 输出参数，被赋值为新创建的Class类的对象数组
 * @param Class: 需创建对象数组的类名
 * @param Num: 需创建对象数组的长度
 * @see 
 * @return void
 */
#define FRIEND_NEW_A(Pointer, Class, Num) \
    do \
    { \
        Pointer = new Class [Num]; \
    } \
    while (0)
#else
#define FRIEND_NEW_A(Pointer, Class, Num) \
    do \
    { \
        try \
        { \
            Pointer = new Class [Num]; \
        } \
        catch (...) \
        { \
            Pointer = NULL; \
        } \
    } \
    while (0)
#endif

/**
 * 把delete关键字转换成函数，并封装异常
 * @param p(Type *): 要释放的对象地址
 * @see 
 * @return void
 */
template <typename Type> inline void gDelete(Type *&rp)
{
    if (rp != NULL)
    {
#ifdef _NO_EXCEPTION
        delete rp;
        rp = NULL;
#else
        try
        {
            delete rp;
            rp = NULL;
        }
        catch (...)
        {

        }
#endif // _NO_EXCEPTION
        rp = NULL;
    }
}

/**
 * 把delete []关键字转换成函数，并封装异常
 * @param p(Type *): 要释放的对象数组地址
 * @see 
 * @return void
 */
template <typename Type> inline void gDeleteA(Type *&rp)
{
    if (rp != NULL)
    {
#ifdef _NO_EXCEPTION
        delete [] rp;
        rp = NULL;
#else
        try
        {
            delete [] rp;
            rp = NULL;
        }
        catch (...)
        {

        }
#endif // _NO_EXCEPTION
        rp = NULL;
    }
}

/**
 * 把对象的析构函数转换成通用函数并封装异常
 * @param p(Type *): 需要被析构的对象地址
 * @see 
 * @return void
 */
template <typename Type> inline void gDestruct(Type *p)
{
#ifdef _NO_EXCEPTION
    p->~Type();
#else
    try
    {
        p->~Type();
    }
    catch (...)
    {

    }
#endif // _NO_EXCEPTION
}

/**
 * 
 * @param p(Type *): 需要释放的内存块地址
 * @see 
 * @return void
 */
template <typename Type> inline void gFree(Type *&rp)
{
    if (rp != NULL)
    {
        free(rp);
        rp = NULL;
    }
}

#ifdef _NO_EXCEPTION
/**
 * 释放对象，封装异常
 * @param Pointer: 需要被释放的对象地址
 * @see 
 * @return void
 */
#define FRIEND_DEL(Pointer) \
    do \
    { \
        if (Pointer != NULL) \
        { \
            delete Pointer; \
            Pointer = NULL; \
        } \
    } \
    while (0)
#else
#define FRIEND_DEL(Pointer) \
    do \
    { \
        if (Pointer != NULL) \
        { \
            try \
            { \
                delete Pointer; \
                Pointer=NULL;\
            } \
            catch (...) \
            { \
                 \
            } \
            Pointer = NULL; \
        } \
    } \
    while (0)
#endif

#ifdef _NO_EXCEPTION
/**
 * 释放对象数组，封装异常
 * @param Pointer: 需要被释放的对象数组地址
 * @see 
 * @return void
 */
#define FRIEND_DEL_A(Pointer) \
    do \
    { \
        if (Pointer != NULL) \
        { \
            delete [] Pointer; \
            Pointer = NULL; \
        } \
    } \
    while (0)
#else
#define FRIEND_DEL_A(Pointer) \
    do \
    { \
        if (Pointer != NULL) \
        { \
            try \
            { \
                delete [] Pointer; \
                Pointer = NULL;\
            } \
            catch (...) \
            { \
                 \
            } \
            Pointer = NULL; \
        } \
    } \
    while (0)
#endif

}//end namespace
#endif // MEMORY_HPP_
