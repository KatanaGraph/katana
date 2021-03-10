#!/bin/sh
# This is a sh INCLUDE file. Source it don't run it.

if [ ! -d "$HOME"/miniconda/bin ]; then
  bash "$HOME/.cache/miniconda/miniconda.sh" -f -b -p "$HOME"/miniconda
fi
export PATH=$HOME/miniconda/bin:$PATH
# shellcheck disable=SC1091
. activate
