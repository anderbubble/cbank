%define name clusterbank
%define version trunk
%define unmangled_version trunk
%define release 1

%define scripts cbank cbank-detail cbank-detail-allocations cbank-detail-charges cbank-detail-holds cbank-detail-refunds cbank-edit cbank-edit-alloctaion cbank-edit-charge cbank-edit-hold cbank-edit-refund cbank-import cbank-import-jobs cbank-list cbank-list-allocations cbank-list-charges cbank-list-holds cbank-list-jobs cbank-list-projects cbank-list-users cbank-new cbank-new-allocation cbank-new-charge cbank-new-hold cbank-new-refund

Summary: Accounting software for networked resources.
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
Vendor: Argonne National Laboratory, MCS <systems@mcs.anl.gov>
Packager: Jonathon Anderson <janderso@alcf.anl.gov>
Url: http://trac.mcs.anl.gov/projects/clusterbank
requires: setuptools, SQLAlchemy

%description
A system for storing allocations, holds, and charges for computational
resources.

%prep
%setup -n %{name}-%{unmangled_version}

%build
pwd
echo $RPM_BUILD_ROOT
python setup.py build
mkdir -p build/wrappers docs/man/man7/build
for script in %{scripts}
do
    wrapc /usr/bin/env PYTHONPATH= /usr/lib/%{name}/bin/$script | cc -o build/wrappers/$script -x c -
done
cp docs/man/man7/*.7 docs/man/man7/build
for page in docs/man/man7/build/*.7; do gzip $page; done

%install
pwd
echo $RPM_BUILD_ROOT
python setup.py install --single-version-externally-managed --root=$RPM_BUILD_ROOT --install-scripts=/usr/lib/%{name}/bin --record=INSTALLED_FILES
for dir in usr/bin etc usr/share/man/man7
do
    mkdir -p $RPM_BUILD_ROOT/$dir
done
cp build/wrappers/* $RPM_BUILD_ROOT/usr/bin
cp etc/clusterbank.conf $RPM_BUILD_ROOT/etc
cp docs/man/man7/build/*.7.gz $RPM_BUILD_ROOT/usr/share/man/man7

%clean
rm -rf $RPM_BUILD_ROOT build docs/man/man7/build

%pre
if ! /usr/bin/getent group clusterbank &>/dev/null
then
    groupadd clusterbank
fi

%post
for script in %{scripts}
do
    chmod g+s /usr/bin/$script
done

%postun
if /usr/bin/getent group clusterbank &>/dev/null
then
    groupdel clusterbank
fi

%files -f INSTALLED_FILES
%defattr(-,root,root)
%attr(755,root,clusterbank) /usr/bin/cbank*
%doc INSTALL CHANGELOG README
%doc /usr/share/man/man7/cbank*.7.gz
%config %attr(660,root,clusterbank) /etc/clusterbank.conf
