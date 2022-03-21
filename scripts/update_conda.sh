#!/bin/bash

set -ex

SCRIPT_PATH=$(realpath "$0")
REPO_ROOT=$(dirname "$SCRIPT_PATH")/..
ENVIRONMENT_NAME=$1

if [ -z "$ENVIRONMENT_NAME" ]
then
  echo "Usage: update_conda.sh <environment name>" >&2
  exit 1
fi

conda env update --name "$ENVIRONMENT_NAME" --file "$REPO_ROOT"/conda_recipe/environment.yml
