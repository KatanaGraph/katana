#!/bin/bash
set -euo pipefail
set -x
conda build . -c conda-forge -c metagraph -c katanagraph/label/dev --output-folder "$@"
