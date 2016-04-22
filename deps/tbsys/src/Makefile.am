source_list=config.cpp defaultrunnable.cpp filequeue.cpp \
            filequeuethread.cpp fileutil.cpp process.cpp \
            queuethread.cpp stringutil.cpp tblog.cpp \
            tbnetutil.cpp tbtimeutil.cpp tbrwlock.cpp\
            Utility.cpp Time.cpp Cond.cpp RecMutex.cpp \
            Shared.cpp TbThread.cpp StaticMutex.cpp Mutex.cpp\
            Exception.cpp ThreadException.cpp CtrlCHandler.cpp\
            Timer.cpp ThreadPool.cpp Service.cpp\
            Network.cpp profiler.cpp bytebuffer.cpp WarningBuffer.cpp 

AM_LDFLAGS="-lpthread -lm -lrt"
lib_LTLIBRARIES=libtbsys.la
libtbsys_la_SOURCES=$(source_list)
libtbsys_la_LDFLAGS=$(AM_LDFLAGS) -static-libgcc
include_HEADERS=atomic.h config.h defaultrunnable.h filequeue.h \
               filequeuethread.h fileutil.h iqueuehandler.h \
               process.h queuethread.h runnable.h stringutil.h \
               tblog.h tbnetutil.h tbsys.h tbtimeutil.h \
               threadcond.h thread.h threadmutex.h linklist.h \
               tbrwlock.h tblockguard.h PublicDefine.h Memory.hpp\
               Cond.h Functional.h Lock.h Network.h EventHandler.h\
               Service.h ThreadException.h Time.h Handle.h Monitor.h\
               RecMutex.h Shared.h TbThread.h Timer.h CtrlCHandler.h\
               Exception.h Mutex.h StaticMutex.h ThreadPool.h Utility.h \
	             profiler.h bytebuffer.h WarningBuffer.h

noinst_PROGRAMS=



