#!/bin/bash
REPO_ROOT="$(cd "$(dirname "$0")/.."; pwd)"

set -euo pipefail

export KATANA_VERSION=$("$REPO_ROOT"/scripts/version show --pep440)

if [ -z "${NO_MAMBABUILD:-}" ] && command -v conda-mambabuild > /dev/null; then
  echo "Using mambabuild (because it's much faster and available). You can disable this by setting NO_MAMBABUILD=true."
  set -x
  conda mambabuild -c conda-forge "$REPO_ROOT"/conda_recipe "$@"
else
  set -x
  conda build -c conda-forge "$REPO_ROOT"/conda_recipe "$@"
fi
