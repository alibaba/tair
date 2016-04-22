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

#ifndef PROFILER_H
#define PROFILER_H

#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <stdint.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <errno.h>

#include "tbsys.h"

#define PROFILER_START(s) do { if(tbsys::util::Profiler::m_profiler.status == 1) tbsys::util::Profiler::m_profiler.start((s)); } while(0)
#define PROFILER_STOP() do { if(tbsys::util::Profiler::m_profiler.status == 1) tbsys::util::Profiler::m_profiler.reset(); } while (0)
#define PROFILER_BEGIN(s) do { if(tbsys::util::Profiler::m_profiler.status == 1) tbsys::util::Profiler::m_profiler.begin((s)); } while(0)
#define PROFILER_END() do { if(tbsys::util::Profiler::m_profiler.status == 1) tbsys::util::Profiler::m_profiler.end(); } while(0)
#define PROFILER_DUMP() do { if(tbsys::util::Profiler::m_profiler.status == 1) tbsys::util::Profiler::m_profiler.dump(); } while(0)
#define PROFILER_SET_THRESHOLD(sh) tbsys::util::Profiler::m_profiler.threshold = (sh)
#define PROFILER_SET_STATUS(st) tbsys::util::Profiler::m_profiler.status = (st)

/**
 * 多线程性能统计工具
 *
 * 使用方法：
 * PROFILER_START("test"); // 初始化一个统计实例
 *
 * PROFILER_BEGIN("entry a"); // 开始一个计时单元
 * PROFILER_END(); // 结束最近的计时单元
 *
 * PROFILER_BEGIN("entry b");
 * PROFILER_BEGIN("sub entry b1"); // 支持嵌套的计时单元
 * PROFILER_END();
 * PROFILER_END()
 *
 * PROFILER_DUMP(); // dump计时记录
 * PROFILER_STOP(); // 结束这个统计实例
 *
 * 配置参数：
 * PROFILER_SET_STATUS(status); // 设置计数器的状态，如果不是1，则禁用计数器所有功能，此时不会产生任何开销，默认为1
 * PROFILER_SET_THRESHOLD(threshold); // 设置dump的阀值，当一个计数实例的总计时超过这个阀值时才会dump信息，单位为us，默认为10000us(10ms)
 */
namespace tbsys { namespace util {

/** 
 * @brief 线程特定数据的创建，获取，设置
 */
template<class T>
class ThreadLocal {
public:
	ThreadLocal () {
	    pthread_key_create(&key, NULL);
	}
	virtual ~ThreadLocal () {}
	T get() { return (T)pthread_getspecific(key); }
	void set(T data) { pthread_setspecific(key, (void *)data); }

private:
	pthread_key_t key;
};

/** 
 * @brief 统计线程执行时间的时间单元
 */
class Entry
{
    public:
	Entry(const std::string& message, Entry *parent, Entry *first) {
	    this->message = message;
	    this->parent = parent;
	    this->first = (first == NULL) ? this : first;
	    btime = (first == NULL) ? 0 : first->stime;
	    stime = Entry::getTime();
	    etime = 0;
	}

	~Entry() {
	    if (!subEntries.empty())
		for (size_t i=0; i<subEntries.size(); i++) 
		    delete subEntries[i];
	}

	long getStartTime() {
	    return (btime > 0) ? (stime - btime) : 0;
	}

	long getDuration() {
	    if (etime >= stime)
		return (etime - stime);
	    else
		return -1;
	}

	long getEndTime() {
	    if (etime >= btime)
		return (etime - btime);
	    else
		return -1;
	}

	long getMyDuration() {
	    long td = getDuration();

	    if (td < 0)
		return -1;
	    else if (subEntries.empty())
		return td;
	    else {
		for (size_t i=0; i<subEntries.size(); i++)
		    td -= subEntries[i]->getDuration();
		return (td < 0) ? -1 : td;
	    }
	}

	double getPercentage() {
	    double pd = 0;
	    double d = getMyDuration();

	    if (!subEntries.empty())
		pd = getDuration();
	    else if (parent && parent->isReleased())
		pd = static_cast<double>(parent->getDuration());

	    if (pd > 0 && d > 0)
		return d / pd;

	    return 0;
	}

	double getPercentageOfTotal() {
	    double fd = 0;
	    double d = getDuration();

	    if (first && first->isReleased())
		fd = static_cast<double>(first->getDuration());

	    if (fd > 0 && d > 0)
		return d / fd;

	    return 0;
	}

	void release() {
	    etime = Entry::getTime();
	}

	bool isReleased() {
	    return etime > 0;
	}

	void doSubEntry(const std::string& message) {
	    Entry *entry = new Entry(message, this, first);
	    subEntries.push_back(entry);
	}

	Entry *getUnreleasedEntry() {
	    if(subEntries.empty()) 
		return NULL;

	    Entry *se = subEntries.back();
	    if(se->isReleased())
		return NULL;
	    else
		return se;
	}

  std::string toString() {
	    return toString("", "");
	}
  std::string toString(const std::string& pre1, const std::string& pre2) {
    std::ostringstream ss;
	    toString(pre1, pre2, ss);
	    return ss.str();
	}

  std::string toString(const std::string& pre1, const std::string& pre2, std::ostringstream &ss) {
	    ss<<pre1;

	    if (isReleased()) {
		char temp[256];
		sprintf(temp, "%lu [%lu(us), %lu(us), %.2f%%, %.2f%%] - %s", getStartTime(), getDuration(), getMyDuration(), getPercentage() * 100, getPercentageOfTotal() * 100, message.c_str());
		ss<<temp;
	    } else {
		ss<<"[UNRELEASED]";
	    }

	    for (size_t i=0; i<subEntries.size(); i++) {
		Entry *ent = subEntries[i];
		ss<<std::endl;

		if (i == 0)
		    ss<<ent->toString(pre2 + "+---", pre2 + "|  ");
		else if (i == (subEntries.size() - 1))
		    ss<<ent->toString(pre2 + "`---", pre2 + "    ");
		else
		    ss<<ent->toString(pre2 + "+---", pre2 + "|   ");
	    }

	    return ss.str();
	}

	static uint64_t getTime() {
	    timeval time;
	    gettimeofday(&time, NULL);
	    return time.tv_sec * 1000000 + time.tv_usec;
	}

  std::string message;
    private:
  std::vector<Entry *> subEntries;
	Entry *parent;
	Entry *first;
	uint64_t stime;
	uint64_t btime;
	uint64_t etime;
};


/** 
 * @brief 多线程性能统计工具
 */
class Profiler
{
    public:
	Profiler();

	void start(const std::string& description);

	void stop();

	void reset();

	void begin(const std::string& description);

	void end();

	long getDuration();

	Entry *getCurrentEntry();

	void dump();

    private:
	ThreadLocal<Entry*> entry;
    public:
	int threshold;
	int status;
	static Profiler m_profiler;
};

} } // end of namespace

#endif /* end of include guard: PROFILER_H */
