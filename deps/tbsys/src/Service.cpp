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

#include "Service.h"
#include "Network.h"
#include "CtrlCHandler.h"

#include <signal.h>
#include <stdio.h>
#include <iostream>
#include <fstream>

using namespace std;

namespace tbutil
{
tbutil::Service* tbutil::Service::_instance=NULL;
static tbutil::CtrlCHandler* _ctrlCHandler=NULL;

static void ctrlCHandlerCallback( int sig )
{
    tbutil::Service* service = tbutil::Service::instance();
    assert( service != 0 );
    service->handleInterrupt( sig );
}

Service::Service():
   _nohup( true),
   _service( false),
   _changeDir( false),
   _closeFiles( false),
   _destroyed( false ),
   _chlidStop(false)
{
   assert(_instance == NULL );
   _instance = this;
}

Service::~Service()
{
    _instance = NULL;
    delete _ctrlCHandler;
}

int Service::main(int argc,char*argv[])
{
    bool daemonize(false);
    bool closeFiles(true);
    bool changeDir(true);
    int idx(1);
    if ( argc < 2 )
    {
        cerr<<":invalid option\n"<<"Try `--help' for more information"<<endl;
        return EXIT_FAILURE;
    }
    while( idx < argc )
    {
        if( strcmp(argv[idx],"-h") == 0 || strcmp(argv[idx],"--help") == 0 )
        {
            help();//show help
            return EXIT_SUCCESS;
        }
        else if ( strcmp(argv[idx],"-v" ) == 0 || strcmp(argv[idx],"--version" ) == 0 )
        {
            version();//show version
            return EXIT_SUCCESS;
        }
        else if( strcmp(argv[idx],"--daemon")== 0 )
        {
            for( int i = idx; i+1 <argc;++i)
            {
                argv[i] = argv[i+1];
            }
            argc -= 1;
            daemonize = true;
        }
        else if( strcmp(argv[idx],"--noclose")==0)
        {
            for( int i = idx; i+1 <argc;++i)
            {
                argv[i] = argv[i+1];
            }
            argc -= 1;
            closeFiles=false; 
        }
        else if( strcmp(argv[idx],"--nochdir")==0)
        {
            for( int i = idx; i+1 <argc;++i)
            {
                argv[i] = argv[i+1];
            }
            argc -= 1;
            changeDir = false;
        }
        else if (  strcmp(argv[idx],"-p" ) == 0 
                || strcmp( argv[idx],"--pidfile") == 0 )
        {
            if( idx + 1 < argc )
            {
                _pidFile = argv[idx + 1];
            }
            else
            {
                cerr<<":--pidfile|-p must be followed by an argument..."<<endl;
                return EXIT_FAILURE;
            }
            for( int i = idx; i+2 <argc;++i)
            {
                argv[i] = argv[i+2];
            }
            argc -= 2;
        }
        else if ( strcmp(argv[idx],"-f" ) == 0 || strcmp( argv[idx],"--config") == 0 )
        {
            if ( idx + 1 < argc )
            {
                _configFile=argv[idx+1];
            }
            else
            {
                cerr<<":--config|-f must be followed by an argument...."<<endl;
                return EXIT_FAILURE;
            }
            for ( int i = idx ; i+2 < argc; ++i )
            {
                argv[i] = argv[i+2];
            }
            argc -=2;
            if ( _configFile.size() < 0 || _configFile.empty() )
            {
                 cerr<<":--config|-f must be followed an argument,argument is not null"<<endl;
                 return EXIT_FAILURE;
            }
        }
        else if ( strcmp( argv[idx],"-k") == 0 || strcmp( argv[idx],"--cmd") == 0 )
        {
            if ( idx + 1 < argc )
            {
                 _cmd = argv[idx + 1];
            }
            else
            {
                 cerr<<":--cmd|-k must be followed by an argument..."<<endl;
                 return EXIT_FAILURE;
            }
            for ( int i = idx ; i+2 < argc; ++i )
            {
                argv[i] = argv[i+2];
            }
            argc -=2;
            if ( _cmd.size() < 0 || _cmd.empty() )
            {
                cerr<<":--cmd|-k must be followed an argument,argument is not null"<<endl;
                return EXIT_FAILURE;
            }
        }
        else if ( strcmp( argv[idx],"--nochstdOut") == 0 )
        {
            for( int i = idx; i+1 <argc;++i)
            {
                argv[i] = argv[i+1];
            }
            argc -= 1;
            _chstdOut +="yes";
        }
        else if (strcmp( argv[idx],"--nochstdErr") == 0 )
        {
            for( int i = idx; i+1 <argc;++i)
            {
                argv[i] = argv[i+1];
            }
            argc -= 1;
            _chstdErr +="yes";
        }
        else if ( strcmp( argv[idx],"--noinit") == 0 )
        {
            for( int i = idx; i+1 <argc;++i)
            {
                argv[i] = argv[i+1];
            }
            argc -= 1;
            _noinit+="yes";
        }
        else
        {
            ++idx;
            cerr<<":invalid option\n"<<"Try `--help' for more information"<<endl;
            return EXIT_FAILURE;
        }//end if
    }//end while idx<argc
    
    if ( !closeFiles && !daemonize )
    {
        cerr<<":--noclose must be whith --daemon..."<<endl;
        return EXIT_FAILURE;
    }
    if ( _pidFile.size() > 0 && !_pidFile.empty() && !daemonize )
    {
        cerr<<":--pidfile|-p must be whith --daemon..."<<endl;
        return EXIT_FAILURE;
    }
    
    if ( daemonize )
    {
        if ( _pidFile.size() <= 0 || _pidFile.empty() )
        {
            cerr<<":--daemon muste be whith --pidfile|-p..."<<endl;
            return EXIT_FAILURE;
        }
        configureDaemon(changeDir,closeFiles);
    }

    if ( _cmd.size() < 0 || _cmd.empty() )
    {
        cerr<<":must be with --cmd|-k,-cmd|-k must be followed an argument[start|stop] ,argument is not null"<<endl;
        return EXIT_FAILURE;
    }

    if ( _cmd.compare("stop") == 0 )
    {
        if ( _pidFile.size() <= 0 || _pidFile.empty() )
        {
            cerr<<":muste be whith --pidfile|-p..."<<endl;
            return EXIT_FAILURE;
        }
        char *p = NULL;
        char buffer[64];
        int otherpid = 0, lfp=-1;
        lfp = open(_pidFile.c_str(), O_RDONLY, 0);
        if (lfp >= 0)
        {
            read(lfp, buffer, 64);
            close(lfp);
            buffer[63] = '\0';
            p = strchr(buffer, '\n');
            if (p != NULL)
                *p = '\0';
            otherpid = atoi(buffer);
            unlink(_pidFile.c_str());//删除pidfile
        }
        else
        {
            cerr<<"[ERROR]: Not found pidfile["<<_pidFile.c_str()<<"]..."<<endl;
        }
        if (otherpid > 0)
        {
            if (kill(otherpid, SIGINT) != 0)
            {
                cerr<<"[ERROR]: kill pid["<<otherpid<<"] failed..."<<endl;
            }
        }
        else
        {
            cerr<<"[ERROR]: Not found pid["<<otherpid<<"]...."<<endl;
        }
        return EXIT_SUCCESS;
    }
    else if (_cmd.compare("start") == 0 )
    {
        if ( _configFile.empty() )
        {
            cerr<<"[ERROR]: must be follow --config|-f Files..."<<endl;
            return EXIT_FAILURE;
        } 
        return start( argc , argv );
    }
    else
    {
        cerr<<":not found --cmd "<<_cmd<<endl;
        cerr<<":invalid option\n"<<"Try `--help' for more information"<<endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;    
}

Service* Service::instance()
{
    return _instance;
}

bool Service::service() const
{
    return _service;
}

int Service::start(int argc , char* argv[] )
{
    if ( _service )
    {
        return runDaemon(argc,argv );
    }
    std::string errMsg; 
    int status = EXIT_FAILURE;
    try
    {
       _ctrlCHandler = new tbutil::CtrlCHandler;
       if ( run( argc , argv,_configFile, errMsg ) == EXIT_SUCCESS )
       {
           cerr<<"[INFO]: Service initialize finished..."<<endl;
           waitForShutdown();

           if ( _chlidStop )
           {
               if ( destroy() )
               {
                   status = EXIT_SUCCESS;
               }
           }
       }
       else
       {
           cerr<<"[ERROR]: Service initialize failure...."<<endl;
       }
    }
    catch(const std::exception& ex)
    {
        cerr<<"service caught unhandled exception:\n"<<ex.what()<<endl;
    }
    catch( const std::string& msg )
    {
        cerr<<"service caught unhandled exception:\n"<<msg<<endl;
    }
    catch( const char* msg )
    {
        cerr<<"service caught unhandled exception:\n"<<msg<<endl;
    }
    catch( ... )
    {
        cerr<<"service caught unhandled C++ exception"<<endl;
    }
    cerr<<errMsg<<endl;
    return status;
}

void Service::stop()
{
    _chlidStop = true;
    shutdown();
}

void Service::configureDaemon(bool changeDir , bool closeFile)
{
    _service = true;
    _changeDir = changeDir;
    _closeFiles=closeFile;
}

int Service::interruptCallback( int sig )
{
    return EXIT_SUCCESS;
}

int Service::shutdown()
{
    Monitor<Mutex>::Lock sync(_monitor);
    if ( !_destroyed )
    {
       _destroyed = true;
       _monitor.notifyAll();
    }
    return EXIT_SUCCESS;
}

int Service::waitForShutdown()
{
    Monitor<Mutex>::Lock sync(_monitor);
    enableInterrupt();
    while( !_destroyed )
    {
        _monitor.wait();
    }
    disableInterrupt();
    return EXIT_SUCCESS;
}

int Service::handleInterrupt(int sig)
{
    if ( _nohup && sig == SIGHUP )
    {
        interruptCallback(sig);
        return EXIT_SUCCESS;
    }
    interruptCallback( sig );
    return shutdown();
}

void Service::help()
{
    std::string options=
         "Options:\n"
         "-h,--help          Show this message...\n"
         "-v,--version       Show porgram version...\n"
         "--daemon           Run as a daemon...\n"
         "--pidfile|-p FILE  Write process Id into FILE...\n"
         "--noclose          Do not close open file descriptors...\n"
         "--nochdir          Do not change the current working directory...\n"
         "--nochstdOut       Do not change stdout...\n"
         "--nochstdErr       Do not change stderr...\n"
         "--config|-f FILE   Configure files...\n"
         "--cmd|-k COMMAND   Run as a command of program\n"
         "         COMMAND:  \n"
         "                   | stop  : Stop  program...\n"
         "                   | start : Start program...\n";

    fprintf(stderr,"Usage:\n%s" ,options.c_str());
}

void Service::version()
{
    fprintf(stderr,"Version:%s","1.0.0\n");
}

int Service::initialize()
{
    return EXIT_SUCCESS;
}

void Service::enableInterrupt()
{
    _ctrlCHandler->setCallback(ctrlCHandlerCallback);
}

void Service::disableInterrupt()
{
    _ctrlCHandler->setCallback(0);
}

int Service::runDaemon( int argc ,char* argv[] )
{
    assert(_service );

    if ( _pidFile.size() <= 0 || _pidFile.empty() )
    {
        cerr<<":muste be whith --pidfile..."<<endl;
        return EXIT_FAILURE;
    }
 
    int otherpid = 0;
    const int lfp = open(_pidFile.c_str(), O_RDONLY, 0);
    if (lfp >= 0)
    {
        char buffer[64];
        ::memset( buffer , 0 , sizeof(buffer));
        read(lfp, buffer, 64);
        close(lfp);
        buffer[63] = '\0';
        char* p = strchr(buffer, '\n');
        if (p != NULL)
            *p = '\0';
        otherpid = atoi(buffer);
    }
    if (otherpid > 0)
    {
        if (kill(otherpid, 0) == 0)
        {
           cout<<"[INFO]: "<<argv[0]<<" already running. pid:"<<otherpid<<endl;
           return EXIT_FAILURE;
        }
    }   
 
    SOCKET fds[2];
    const int iRet = tbutilInternal::createPipe(fds);
    if ( iRet != 0 )
    {
        cerr<<"Create a pipe failure"<<endl;
        return EXIT_FAILURE;
    }    
    
    // fork the child
    pid_t pid = fork();
    if ( pid < 0 )
    {
        cerr<<":"<<strerror(errno)<<endl;
        return EXIT_FAILURE;
    }
    // parent process
    if ( pid != 0 )
    {
        close(fds[1]);
        char c = 0;
        while( true )
        {
            if ( read(fds[0] , &c , 1 ) == -1 )
            {
                if( tbutilInternal::interrupted() )
                {
                   continue;
                } 
                cerr<<":"<<strerror( errno ) <<endl;
                _exit(EXIT_FAILURE);
            }
            break;
        }
        
        if ( c != 0 )
        {
            char msg[1024];
            ::memset(&msg , 0 , sizeof( msg ) );
            size_t pos=0;
            while( pos < sizeof(msg))
            {
                ssize_t n = read(fds[0],&msg , sizeof(msg)-pos );
                if ( -1 == n )
                {
                    if (tbutilInternal::interrupted() )
                    {
                        continue;
                    }
                    cerr<<":I/O error while reading error message from child process:\n"
                        <<strerror( errno ) <<endl;
                    _exit(EXIT_FAILURE);
                }
                pos += n;
                break;
            }
            cerr <<"[ERROR]: failure occurred in daemon"<<endl;
            if ( strlen(msg) > 0 )
            {
                cerr<<msg;
            }
            cerr<<endl;
            _exit(EXIT_FAILURE );
        }//end if c != 0
        _exit( EXIT_SUCCESS );
    }//end pid != 0

    //child process
    std::string errMsg;
    std::string msg;
    int32_t status = EXIT_FAILURE;
    try
    {
        if ( setsid() == -1 )
        {
            SyscallException ex(__FILE__,__LINE__);
            ex._error= getSystemErrno();
            throw ex; 
        }
        signal(SIGHUP , SIG_IGN); // ignore SIGHUP
        
        pid = fork();
        if ( pid < 0 )
        {
            SyscallException ex(__FILE__,__LINE__);
            ex._error= getSystemErrno();
            throw ex; 
        }
        
        if ( pid != 0 )//parent process
        {
            exit(EXIT_SUCCESS);
        }
    
        if ( _changeDir )
        {
            if (chdir("/") != 0 )
            {
                SyscallException ex(__FILE__,__LINE__);
                ex._error= getSystemErrno();
                throw ex; 
            }
        }
    
        vector<int> fdsToClose;
        if ( _closeFiles )
        {
            int fdMax = static_cast<int>(sysconf(_SC_OPEN_MAX));
            if ( fdMax <= 0 )
            {
                SyscallException ex(__FILE__,__LINE__);
                ex._error= getSystemErrno();
                throw ex; 
            }
    
            for(int i = 0; i < fdMax; ++i )
            {
                if(fcntl(i, F_GETFL) != -1 )
                {
                    if ( i != fds[1] )//fds[1]: pipe 
                    {
                        fdsToClose.push_back(i);
                    }
                }
            }
        }
        
        _ctrlCHandler = new tbutil::CtrlCHandler();
       
        if ( _closeFiles )
        {
            vector<int>::const_iterator iter;
            for ( iter = fdsToClose.begin(); iter != fdsToClose.end(); ++iter )
            {
                 if ((*iter == 1 && !_chstdOut.empty())
                     ||(*iter == 2 && !_chstdErr.empty() ) )
                 {
                     continue;
                 }
                 close(*iter);
            }
    
            int fd = open("/dev/null",O_RDWR );
            assert( fd == 0 );
            if ( _chstdOut.empty() )
            {
                fd = dup2(0,1);
                assert( fd == 1 );
            }
            if ( _chstdErr.empty() )
            {
                fd = dup2(1,2);
                assert( fd == 2 );
            }
        }//end if _closeFiles
    
        // Write pid
    
        /*if ( _pidFile.size() > 0 )
        {
           ofstream of(_pidFile.c_str() );
           of<<getpid()<<endl;
           if ( !of )
           {
              cerr<<"[WARN]: Could not write PID file:"<<_pidFile<<endl;
           }
        }*/
   
        const int iRet2 = run(argc , argv ,_configFile, msg);
        if ( iRet2 == EXIT_SUCCESS )
        {
            char c = 0;
            while( true )
            {
                if( write(fds[1] , &c , 1 ) == -1 )
                {
                    if ( tbutilInternal::interrupted() )
                    {
                        continue;
                    }
                }
                break;
            }
            close( fds[1] );
            fds[1] = -1;
    
            if ( _noinit.empty() )
            {
                initialize();
            }       
            //Write pid
            if ( _pidFile.size() > 0 )
            {
                ofstream of(_pidFile.c_str() );
                of<<getpid()<<endl;
                if ( !of )
                {
                    cerr<<"[WARN]: Could not write PID file:"<<_pidFile<<endl;
                }
            }    
 
            waitForShutdown();
    
            if ( _chlidStop )
            {
                if ( destroy() )
                {
                    status = EXIT_SUCCESS;
                }
            }
        }
        else
        {
            errMsg ="[ERROR]: Service initialize failure....";
        } 
    }
    catch(const Exception& ex)
    {
        ostringstream str;
        str<<"service caught unhandled exception:\n"<<ex.what();
        errMsg = str.str();
    }
    catch(...)
    {
        errMsg = "service caught unhandled C++ exception";
    }

    if ( status != EXIT_SUCCESS && fds[1] != -1 )
    {
       while( true )
       {
           char c = 1;
           if(write(fds[1] ,& c , 1 ) == -1 )
           {
               if(tbutilInternal::interrupted() )
               {
                  continue;
               }
           }
           break;
       }
      
       errMsg += "\r\n";
       errMsg += msg;
        
       const char* msg = errMsg.c_str();
           
       size_t iLen = strlen( msg ) + 1;
       size_t pos = 0;
       while( iLen > 0 )
       {
           ssize_t n = write(fds[1] , &msg[pos] , iLen );
           if ( n == -1 )
           {
               if ( tbutilInternal::interrupted() )
               {
                   continue;
               }
               else
               {
                   break;
               }
           }
           iLen-= n;
           pos += n;
       }
       close(fds[1] );
    }
    return status;
}
}

