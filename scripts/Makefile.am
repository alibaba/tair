pkgdata_datadir=${prefix}/
pkgdata_data_DATA=do_dump.sh do_upload.sh tair.sh set_shm.sh
EXTRA_DIST=do_dump.sh do_upload.sh tair.sh set_shm.sh

install-data-hook:
	for f in ${EXTRA_DIST}; \
	do \
	  chmod +x ${DESTDIR}/${prefix}/$$f ;\
	done

