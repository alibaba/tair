if TEST_OPT
TEST = test
endif
SUBDIRS=src scripts share $(TEST)
DIST_SUBDIRS = src scripts share test

utest: check

.PHONY: utest
EXTRA_DIST=bootstrap.sh \
	   share/configserver.conf share/dataserver.conf share/group.conf share/Makefile.am \
	   share/Makefile.in \
	   scripts/Makefile.am scripts/Makefile.in scripts/mdbshm_reader.py scripts/set_shm.sh \
	   scripts/tair.sh \
	   libtool \
	   packages/tair.spec 

TMP_DIR:=/tmp/tair-tmp.$(shell echo $$$$)
PREFIX:=$(shell [ x$(PREFIX) = x ] && PREFIX=/opt/csr/tair-$(VERSION) && echo "$${PREFIX}")
BOOST_DIR:=$(shell [ x$(BOOST_DIR) = x ] && BOOST_DIR=/usr && echo "$${BOOST_DIR}")
rpms: dist-gzip packages/tair.spec
	mkdir -p $(TMP_DIR)
	mkdir -p $(TMP_DIR)/BUILD
	mkdir -p $(TMP_DIR)/RPMS
	mkdir -p $(TMP_DIR)/SOURCES
	mkdir -p $(TMP_DIR)/SRPMS
	cp tair-${VERSION}.tar.gz $(TMP_DIR)/SOURCES
	echo $(PREFIX)
	echo $(BOOST_DIR)
	cd $(TMP_DIR)/BUILD; \
	tar xfz $(TMP_DIR)/SOURCES/tair-${VERSION}.tar.gz \
		tair-$(VERSION)/packages/tair.spec
	rpmbuild --define "_topdir $(TMP_DIR)" \
		 --define "NAME $(PACKAGE)" \
		 --define "VERSION $(VERSION)" \
		 --define "BOOST_DIR $(BOOST_DIR)" \
		 --define "_prefix $(PREFIX)" \
		-ba $(TMP_DIR)/BUILD/tair-$(VERSION)/packages/tair.spec
	find $(TMP_DIR)/RPMS/ -name "*.rpm" -exec mv '{}' . \;
	rm -rf $(TMP_DIR)

