#!/bin/bash
set -xeuo pipefail

MINICONDA_FILE="$HOME/.cache/miniconda/mambaforge.sh"
case $1 in
  ubuntu-*) PLAT=Linux-x86_64 ;;
  *)        echo "Unknown OS"; exit 10 ;;
esac

VER="4.11.0-0"

INPUT_URL="https://github.com/conda-forge/miniforge/releases/download/${VER}/Mambaforge-${VER}-${PLAT}.sh"
mkdir -p "$(dirname $MINICONDA_FILE)"
curl -fL --output "$MINICONDA_FILE" "$INPUT_URL"
