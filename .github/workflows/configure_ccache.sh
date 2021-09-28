#!/bin/bash
set -xeuo pipefail

# Explicitly set location to make sure transferring the cache works if that is happening.
ccache -o cache_dir=$HOME/.ccache

# Use the compiler version in hash instead of mtime, this allows caching even when compiler is reinstalled and
# installer doesn't set mtime (conda has this issue).
ccache -o compiler_check='%compiler% --version'

# Hash current directory. The current directory only affect debug information, but for safety hash it.
#  If we still have too many misses we can disable this and see if it helps.
ccache -o hash_dir=true

ccache --zero-stats
