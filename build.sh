#!/bin/bash
set -e

build_dir=${1:-out}
built_type=${2:-release}


case "$built_type" in
  debug)
    opt=" -DCMAKE_BUILD_TYPE=DEBUG -DWITH_DEBUG=yes "
    ;;
  debug_info)
    opt=" -DCMAKE_BUILD_TYPE=RELWITHDEBINFO -DWITH_DEBUG=OFF "
    ;;
  release)
    opt=" -DCMAKE_BUILD_TYPE=RELEASE -DWITH_DEBUG=OFF "
    ;;
  *)
    echo "Usage: $0 BUILD_DIR BUILD_TYPE"
    echo "BUILD_TYPE: debug|debug_info|release"
    exit 1
esac

test -d "$build_dir" || mkdir -p "$build_dir"

cd "$build_dir"

cmake  -DWITHOUT_TOKUDB=0 \
       -DWITH_ARIA_STORAGE_ENGINE=OFF \
       -DWITH_PERFSCHEMA_STORAGE_ENGINE=0 \
       -DWITH_INNOBASE_STORAGE_ENGINE=OFF \
       -DCMAKE_INSTALL_PREFIX=./mysql  -DMYSQL_DATADIR=./mysql/data \
       -DCMAKE_CXX_FLAGS=-std=c++11 \
       -DWITH_SSL=system \
       -DWITH_ZLIB=system \
       -DWITH_WSREP=OFF \
       -DWITH_PCRE=bundled \
       $opt \
       -Wno-deprecated ..

make -j`nproc` install

if test -x sql/arkproxy; then
  echo -n "Build result: "; readlink -f sql/arkproxy
else
  echo "Build failed!"
  exit 1
fi

