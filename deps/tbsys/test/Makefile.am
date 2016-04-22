AM_CPPFLAGS=-I$(top_srcdir)/src
LDADD=$(top_srcdir)/src/.libs/libtbsys.a 
AM_LDFLAGS="-lrt -lm -lpthread"

noinst_PROGRAMS=testfilequeue testqueuethread testconfig \
                teststringutil testnetutil testlog \
                testfileutil testtimeutil testthread\
                testtimer testthreadpool testService \
								testwarningbuffer

testfilequeue_SOURCES=testfilequeue.cpp
testqueuethread_SOURCES=testqueuethread.cpp
testconfig_SOURCES=testconfig.cpp
teststringutil_SOURCES=teststringutil.cpp
testnetutil_SOURCES=testnetutil.cpp
testlog_SOURCES=testlog.cpp
testfileutil_SOURCES=testfileutil.cpp
testtimeutil_SOURCES=testtimeutil.cpp
testthread_SOURCES=testBase.cpp testthread.cpp 
testtimer_SOURCES=testBase.cpp testTimer.cpp 
testthreadpool_SOURCES=testBase.cpp testThreadPool.cpp 
testService_SOURCES=testBase.cpp testService.cpp
testwarningbuffer_SOURCES=testwarningbuffer.cpp
