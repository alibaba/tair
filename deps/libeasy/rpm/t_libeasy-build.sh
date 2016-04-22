#!/bin/bash

export LANG=C
usage()
{
  echo "Usage: $0 rpmdir packagename version release"
  exit 0
}
if [ $# -ne 4 ]; then
  usage
fi

RPM_MACROS=$HOME/.rpmmacros
if [ -e $RPM_MACROS ]; then
  mv -f $RPM_MACROS $RPM_MACROS.bak
fi

TOPDIR=/tmp/.rpm_create_$$
version=$3
dist=`cat /etc/redhat-release | awk '{print int($7)}'`
if [ "$dist" == "4" ] || [ "$dist" == "5" ] || [ "$dist" == "6" ]; then
  release=$4.el${dist}
else
  release=$4
fi
svn_version=`svn info .. 2>&1 | grep Revision: | cut -d " " -f 2`

echo "%_topdir $TOPDIR" > $RPM_MACROS
echo "%_libeasy_version $version" >> $RPM_MACROS
echo "%_libeasy_sover   ${version%.*}" >> $RPM_MACROS
echo "%_svn_version $svn_version" >> $RPM_MACROS
echo "%_release $release" >> $RPM_MACROS

cd ..
rm -f rpm/t_libeasy*.rpm
rm -rf "${TOPDIR}"
mkdir -p "${TOPDIR}/SPECS" "${TOPDIR}/SOURCES" "${TOPDIR}/SRPMS" "${TOPDIR}/RPMS" "${TOPDIR}/BUILD"
/bin/cp -f configure.ac configure.ac.rpmbak
/bin/cp -f src/libeasy.map src/libeasy.map.rpmbak 
/bin/cp -f src/Makefile.am src/Makefile.am.rpmbak 
sed -i -e "/AC_INIT/ s/t_libeasy, [^,]\+,/t_libeasy, ${version}-${release},/" configure.ac
sed -i -e "s/^EASY_.*{/EASY_${version%.*} {/g" src/libeasy.map
sed -i -e "/version-number/ s/version-number.*/version-number ${version//./:}/g" src/Makefile.am
sh bootstrap.sh
./configure
make rpm
/bin/cp -f configure.ac.rpmbak configure.ac
/bin/cp -f src/libeasy.map.rpmbak src/libeasy.map 
/bin/cp -f src/Makefile.am.rpmbak src/Makefile.am 

rpmlist=`find $TOPDIR/RPMS -name "*.rpm"`
for rpm in $rpmlist
do
  echo "move $rpm"
  mv $rpm rpm/
done

rm -rf $TOPDIR $RPM_MACROS
if [ -f $RPM_MACROS.bak ]; then
  mv -f $RPM_MACROS.bak $RPM_MACROS
fi
cd -
