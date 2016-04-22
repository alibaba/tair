AUTOMAKE_OPTIONS = foreign
SUBDIRS = src test sample

RPMSOURCEDIR = $(shell rpm --eval '%_sourcedir')
RPMSPECDIR   = $(shell rpm --eval '%_specdir')
RPMSRCRPMDIR = $(shell rpm --eval '%_srcrpmdir')
RELEASE      = $(shell rpm --eval '%_release')
VERSION      = $(shell rpm --eval '%_libeasy_version')
SOVER        = $(shell rpm --eval '%_libeasy_sover')
SVN_REVISION = $(shell rpm --eval '%_svn_version')

srpm: dist
	sed -e "s/EASY_VERSION/$(VERSION)/g" \
	    -e "s/EASY_RELEASE/$(RELEASE)/g" \
	    -e "s/EASY_SOVER/$(SOVER)/g" \
	    -e "s/SVN_REVISION/$(SVN_REVISION)/g" < rpm/libeasy.spec.in > $(RPMSPECDIR)/$(PACKAGE).spec
	cp $(PACKAGE)-$(VERSION).tar.gz $(RPMSOURCEDIR)
	rpmbuild -bs $(RPMSPECDIR)/$(PACKAGE).spec

rpm: srpm
	rpmbuild --rebuild $(RPMSRCRPMDIR)/$(PACKAGE)-$(VERSION)-$(RELEASE).src.rpm
