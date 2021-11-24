#!/bin/bash
#
# Run from build directory to uprev the storage format version of the input rdgs

#TODO(emcginnis): considered porting this to python

usage="./uprev_rdg_storage_format_version.sh <storage_format_version>"
example="./uprev_rdg_storage_format_version.sh 3"

if [ -z "$1" ]; then
    echo "Must specify new storage format version" >&2
    echo "usage: $usage"
    echo "example: $example"
    exit 1
fi

set -eu -o pipefail

if [[ ! -f CMakeCache.txt ]]; then
    echo run from build directory >&2
    exit 1
fi

NEW_VERSION=$1

NEW_VERSION_DIR=storage_format_version_$NEW_VERSION/
CURRENT_VERSION_DIR=current_storage_format_version
BUILD_DIR=$(pwd)
WORKER=$BUILD_DIR/tools/uprev-rdg-storage-format-version-worker/uprev-rdg-storage-format-version-worker
RDG_TEST_INPUTS=$BUILD_DIR/inputs/current/rdg-test-inputs
WORKER_OUT=$RDG_TEST_INPUTS/$NEW_VERSION_DIR

if [[ ! -d $RDG_TEST_INPUTS ]]; then
    echo "$RDG_TEST_INPUTS does not exist, must run 'make input' first" >&2
    exit 1
fi

cd $RDG_TEST_INPUTS

if [[ -d $WORKER_OUT ]]; then
    echo "new version directory $WORKER_OUT already exists" >&2
    exit 1
fi

mkdir $WORKER_OUT

# assume the storage_format_version_1 inputs are sane, uprev those
cd storage_format_version_1

count=$(ls -l | wc -l)
echo "upreving $count rdgs"

# create the new upreved rdgs
for dirname in */ ; do
    echo "upreving $dirname to $NEW_VERSION_DIR"
    $WORKER $dirname $WORKER_OUT/$dirname
done


# ensure we created as many rdgs as we expected to
cd $WORKER_OUT
new_count=$(ls -l | wc -l)

if [ $count != $new_count ]; then
    echo "Expected to create $count rdgs, but only observe $new_count"
    exit 1
fi

# update the current_storage_format_version symlink
cd $RDG_TEST_INPUTS
ln -sfn $NEW_VERSION_DIR $CURRENT_VERSION_DIR

echo "Default rdg inputs are now $NEW_VERSION_DIR"



