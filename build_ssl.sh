#!/bin/bash

debug_dir=$1
platform=$2

if [ $# -eq 1 ]
then 
	echo "building project in $1"
	platform="x"
elif [ $# -ne 2 ]
then
	echo "Usage: $0 builddir [platform(debug:release_info:release:Xcode)]"
	echo "EXAPMLE: $0 debug [Xcode]"
	exit
fi

Gplatform=""
makerule=""
if [ $platform == "Xcode" ]
then
    Gplatform="-G Xcode"
else
    if [ $platform == "debug" ]; then
        Gplatform="-DWITH_DEBUG=yes -DCMAKE_BUILD_TYPE=DEBUG"
    elif [ $platform == "release_info" ]; then
        Gplatform="-DWITH_DEBUG=OFF -DCMAKE_BUILD_TYPE=RELWITHDEBINFO "
    elif [ $platform == "release" ]; then
        Gplatform="-DWITH_DEBUG=OFF -DCMAKE_BUILD_TYPE=RELEASE "
    else
    	  echo "Usage: $0 builddir [platform(debug:release_info:release:Xcode)]"
	      echo "EXAPMLE: $0 debug [Xcode]"
	      exit
    fi
    makerule="make -j 80 install"
fi

if [ -d $debug_dir ]
then
  cd $debug_dir
else
  mkdir $debug_dir
  cd $debug_dir
fi

       #-DWITH_PARTITION_STORAGE_ENGINE=0 \
       #-DWITH_XTRADB_STORAGE_ENGINE=0 \
       #-DUSE_ARIA_FOR_TMP_TABLES=0 \
# cmake && make && make install
cmake  -DWITHOUT_TOKUDB=0 -DWITH_ZLIB=system \
       -DWITH_ARIA_STORAGE_ENGINE=OFF \
	   -DWITH_PERFSCHEMA_STORAGE_ENGINE=0 \
       -DWITH_INNOBASE_STORAGE_ENGINE=OFF \
       -DCMAKE_INSTALL_PREFIX=./mysql  -DMYSQL_DATADIR=./mysql/data \
       -DCMAKE_CXX_FLAGS=-std=c++11 \
       -DCMAKE_BUILD_TYPE=RELEASE -DWITH_ZLIB=bundled \
       -DWITH_SSL=/data/cole/openssl \
       -DOPENSSL_SSL_LIBRARY=/data/cole/openssl/lib/libssl.a \
       -DOPENSS_CRYPTO_LIBRARY=/data/cole/openssl/lib/libcrypto.a \ -DCMAKE_BUILD_TYPE=RELEASE -DWITH_ZLIB=bundled \
       -DWITH_WSREP=OFF -DWITH_PCRE=bundled $Gplatform\
  .. -Wno-deprecated

if [ $platform != "Xcode" ]
then
    $makerule
fi
