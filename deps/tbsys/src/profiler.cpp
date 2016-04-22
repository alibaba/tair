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

#include "profiler.h"

namespace tbsys
{
    namespace util
    {
	Profiler Profiler::m_profiler;

	Profiler::Profiler() {
	    threshold = 10000; // 10ms
	    status = 1; // default enable
	}

	void Profiler::start(const string& description) {
	    if (entry.get() != NULL)
		reset();

	    entry.set(new Entry(description, NULL, NULL));
	}

	void Profiler::stop() {
	    end();
	}

	void Profiler::reset() {
	    if(entry.get() != NULL) {
		delete entry.get();
		entry.set(NULL);
	    }
	}

	void Profiler::begin(const string& description) {
	    Entry *ce = getCurrentEntry();
	    if(ce != NULL)
		ce->doSubEntry(description);
	}

	void Profiler::end() {
	    Entry *ce = getCurrentEntry();
	    if(ce != NULL)
		ce->release();
	}

	long Profiler::getDuration() {
	    return entry.get()->getDuration();
	}

	Entry *Profiler::getCurrentEntry() {
	    Entry *se = entry.get();
	    Entry *e = NULL;

	    if (se != NULL) {
		do {
		    e = se;
		    se = e->getUnreleasedEntry();
		} while (se != NULL);
	    }

	    return e;
	}

	void Profiler::dump() {
		Profiler::end();
		Entry *ent = entry.get();
		if (ent != NULL && ent->getDuration() >= threshold && Profiler::status == 1)
			TBSYS_LOG(WARN, "%s", ent->toString().c_str());
	}
	}
}
