#!/bin/bash

mamba env update --file $(dirname "$0")/../conda_recipe/pytorch_deps_environment.yml
git clone --recursive --depth 1 --branch v1.10.0 https://github.com/pytorch/pytorch
cd pytorch
python3 setup.py install
cd ..
rm -r -f pytorch