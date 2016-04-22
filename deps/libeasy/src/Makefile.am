LIBDIRS = util thread io packet
SUBDIRS = include $(LIBDIRS) 
EXTRA_DIST = libeasy.map

LDFLAGS += -Wl,--version-script=${VERSION_MAP} -version-number 1:0:17
lib_LTLIBRARIES=libeasy.la
libeasy_la_SOURCES=
libeasy_la_LIBADD=$(foreach sub,$(LIBDIRS),$(sub)/libeasy_$(sub).la)

