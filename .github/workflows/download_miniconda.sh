#!/bin/bash
set -xeuo pipefail

MINICONDA_FILE="$HOME/.cache/miniconda/miniconda.sh"
case $1 in
  ubuntu-*) PLAT=Linux-x86_64 ;;
  macOS-*)  PLAT=MacOSX-x86_64 ;;
  *)        echo "Unknown OS"; exit 10 ;;
esac

INPUT_URL="https://repo.anaconda.com/miniconda/Miniconda3-py38_4.8.3-${PLAT}.sh"
mkdir -p "$(dirname $MINICONDA_FILE)"
curl -fL --output "$MINICONDA_FILE" "$INPUT_URL"
