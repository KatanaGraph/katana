#!/bin/bash
set -eou pipefail

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 gpu_conf_file"
  exit 1
fi

if ! [ -f "$1" ]; then
  echo "Configuration file does not exist"
  exit 1
fi

export AVA_CHANNEL="TCP"
export AVA_WPOOL="TRUE"

if ! [ -f "../generated/worker" ]; then
  cd ../generated && make R=1 && cd -
fi
sudo -E ./build/manager -f $1
