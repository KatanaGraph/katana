#/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
AVAPATH=$SCRIPTPATH/../ava

if [[ ! -d $AVAPATH ]]; then
  bash $SCRIPTPATH/setup.sh
fi

echo "Generating API remoting stubs (pagerank)..."
cd $AVAPATH/cava
./nwcc $SCRIPTPATH/../specs/cuda_10_1.c -I /usr/local/cuda-10.1/include -I $AVAPATH/cava/headers \
  `pkg-config --cflags glib-2.0`

if [[ ! -L $SCRIPTPATH/../generated ]]; then
  ln -s $AVAPATH/cava/katana_cuda_nw $SCRIPTPATH/../generated
fi

if [[ -d $SCRIPTPATH/../generated ]]; then
  cd $SCRIPTPATH/../generated
  make clean
  make R=1
  ln -s libguestlib.so libcudart.so.10.1
fi
