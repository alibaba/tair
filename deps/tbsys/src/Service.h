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

#ifndef TBSYS_SERVICE_H
#define TBSYS_SERVICE_H
#include<string>
#include "Mutex.h"
#include "Monitor.h"
namespace tbutil
{
/** 
* @brief Service 是对linux/Unix守护进程和将进程运行在控制台时检测CtlC的信号1封装
* 它是一个抽象的单体类,实例化时必须继承此类，并实现run,destroy函数,同时Service是
* 对进程启动和关闭进行封装
*/
class Service
{
public:
    Service();
    virtual ~Service();

    /** 
     * @brief 扫描参数向量，看其中是否有保留的选项、让程序作为系统服务运行。
     * 还有一些保留选项可用于管理任务。决定让程序按(deamon)系统程序运行,还是按照控制台程序运行
     * 生成一个CtrlCHandler ，以进行适当的信号处理
     * 调用start 成员函数。如果start 返回表示失败的false程序将退出 
     * @param argc
     * @param argv[]
     * 
     * @return 
     */
    int main(int argc, char* argv[]);

    static Service* instance();    
    /** 
     * @brief 是否是守护进程(deamon)
     * 
     * @return 
     */
    bool service() const;
    /** 
     * @brief 信号处理器调用它来表示收到了信号,使用此类的用户不能调用此函数
     * 
     * @param sig :信号的编号
     * 
     * @return 
     */
    int handleInterrupt(int sig);

protected:
    /** 
     * @brief 允许子类通过反向控制的方法停止程序
     * 调用此函数时，会调用shutdown
     */
    void stop();
    /** 
     * @brief 让服务器开始关闭的过程 
     * * 调用此函数时会向等待进程终止的条件变量发一个通知
     * @return 
     */
    int shutdown();
    /** 
     * @brief 允许子类执行它的启动活动，比如扫描所提供的参数向量、识别命令行选项
     * 
     * @param argc
     * @param argv[]
     * @param config
     * 
     * @return 
     */
    virtual int run( int argc , char*argv[], const std::string& config, std::string& errMsg)=0;
    /** 
     * @brief 允许子类收到信号时作相应的动作,例如当收到SIGINT时，清理系统使用过的
     * 相关资源，并关闭进程
     * 
     * @param sig
     * 
     * @return 
     */
    virtual int interruptCallback( int sig );
    /** 
     * @brief 在进程关闭时,清理相关的资源
     * 
     * @return 
     */
    virtual bool destroy()=0;
    /** 
     * @brief 显示命令行帮助信息
     */
    virtual void help();
    /** 
     * @brief 显示当前程序版本信息
     */
    virtual void version();

    /** 
    * @brief 启用信号处理行为,启用以后，如果有信号发生interruptCallback函数会被调用 
    */
    void enableInterrupt();
    /** 
    * @brief 禁用信号处理行为,禁用以后，如果有信号发生interruptCallback函数不会被调用 
    */
    void disableInterrupt();

private:
    /** 
     * @brief 将当前进程放入后台运行 
     * 主要处理将程序设置为守护进程相关的工作
     * 创建一个后台子进程,在子进程成功调用run方法之前，父进程不会退出
     * 将子进程的pid写入pid文件中
     * 根据配置改变子进程的工作目录
     * 根据配置关闭没用的文件
     * @param argc
     * @param argv[]
     * 
     * @return 
     */
    int runDaemon( int argc ,char* argv[] );
    /** 
     * @brief 无限期地等待服务关闭
     * 
     * @return 
     */
    int waitForShutdown();
    /** 
     * @brief 配置是否改变当前工作目录以及关闭无用的文件描述符
     * 并将当前程序设置为deamon方法的运行 
     * 
     * @param changeDir
     * @param closeFile
     */
    void configureDaemon( bool changeDir , bool closeFile);
    /** 
     * @brief 根据当前进程运行的方法进行相关的处理
     * 如果是deamon方法运行则调用runDaemon,再由runDaemon调用run方法
     * 将当前程序放入后台运行
     * 如果是控制台程序刚直接调用run方法 
     * 
     * @param argc
     * @param argv[]
     * 
     * @return 
     */
    int start(int argc , char* argv[] );
    /** 
     * @brief 暂时不用
     * 
     * @return 
     */
    virtual int  initialize(); 
private:
    bool _nohup;
    /** 
     * @brief 是否是守护进程 
     */
    bool _service;
    /** 
     * @brief 是否改变工作目录
     */
    bool _changeDir;
    /** 
     * @brief 是否关闭没用的文件
     */
    bool _closeFiles;
    /** 
     * @brief 是否关闭
     */
    bool _destroyed;
    /** 
     * @brief 是否由子类调用stop函数
     */
    bool _chlidStop;
    //std::string _name;
    /** 
     * @brief 进程pid文件
     */
    std::string _pidFile;
    /** 
     * @brief 进程配置文件路径
     */
    std::string _configFile;
    /** 
     * @brief 当前命令行的命令(stop|start)
     */
    std::string _cmd;
    std::string _chstdOut;
    std::string _chstdErr;
    std::string _noinit;
    tbutil::Monitor<tbutil::Mutex> _monitor;
    static Service* _instance;
};//end Servcie
}//end namespace tbutil
#endif
