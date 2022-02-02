#!/bin/bash

export GIT_ROOT=$(readlink -f $(dirname $0)/..)

BASE="$(cd "$(dirname "$(readlink -f "$0")")"; pwd)"
"$BASE"/check_python.py --black --isort "$@"
