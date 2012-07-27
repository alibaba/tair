bin_SCRIPTS=mdbshm_reader.py
pkgdata_datadir=${prefix}/
pkgdata_data_DATA=tair.sh set_shm.sh stat.sh check_version.sh
EXTRA_DIST=tair.sh set_shm.sh stat.sh check_version.sh

install-data-hook:
	for f in ${EXTRA_DIST}; \
	do \
	  chmod +x ${DESTDIR}/${prefix}/$$f ;\
	done
