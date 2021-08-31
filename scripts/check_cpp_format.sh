#!/bin/bash

BASE="$(cd "$(dirname "$0")"; pwd)"
"$BASE"/check_cpp_format.py "$@"
