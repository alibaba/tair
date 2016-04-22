%define name      libeasy 
%define summary   high performance server framework 
%define license   Commercial
%define group     Libraries
%define source    %{name}-%{version}.tar.gz 
%define vendor    Taobao, Alibaba
%define packager  duolong 
%define _prefix   /usr 


Name:      %{name}
Version:1.0.23
Release:   1.el5
Packager:  %{packager}
Vendor:    %{vendor}
License:   %{license}
Summary:   %{summary}
Group:     %{group}
URL:       http://svn.taobao-develop.com/repos/ttsc/trunk/easy/Revision_3769
Source0:   %{source}
Prefix:    %{_prefix}
Buildroot: %{_tmppath}/%{name}-%{version}-root 

#BuildRequires:
#Requires
#Prereq:

%description

%prep
%setup -q

%build
export CFLAGS="-DNODEBUG -fPIE -O3 -g" 
%configure
make %{?_smp_mflags}

%install
%makeinstall

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr/include/easy/easy_atomic.h
/usr/include/easy/easy_baseth_pool.h
/usr/include/easy/easy_buf.h
/usr/include/easy/easy_client.h
/usr/include/easy/easy_connection.h
/usr/include/easy/easy_define.h
/usr/include/easy/easy_file.h
/usr/include/easy/easy_hash.h
/usr/include/easy/easy_http_handler.h
/usr/include/easy/easy_inet.h
/usr/include/easy/easy_io.h
/usr/include/easy/easy_io_struct.h
/usr/include/easy/easy_list.h
/usr/include/easy/easy_log.h
/usr/include/easy/easy_mem_page.h
/usr/include/easy/easy_mem_slab.h
/usr/include/easy/easy_message.h
/usr/include/easy/easy_pool.h
/usr/include/easy/easy_request.h
/usr/include/easy/easy_simple_handler.h
/usr/include/easy/easy_socket.h
/usr/include/easy/easy_string.h
/usr/include/easy/easy_uthread.h
/usr/include/easy/ev.h
/usr/include/easy/http_parser.h
/usr/include/easy/easy_array.h
/usr/include/easy/easy_kfc_handler.h
/usr/include/easy/easy_mem_pool.h
/usr/include/easy/easy_tbnet.h
/usr/include/easy/easy_time.h
/usr/lib64/libeasy.a
/usr/lib64/libeasy.la
/usr/lib64/libeasy.so
/usr/lib64/libeasy.so.0
/usr/lib64/libeasy.so.0.0.0

%changelog

