#!/bin/bash

# Just in case add .local/bin to PATH (to pick up pip install programs)

export PATH="$PATH:$HOME/.local/bin"

echo "DO NOT WORRY ABOUT THIS CONAN WARNING. It's handled correctly by the script."
# TODO: Can we suppress the warning? It's rather distressing
conan profile new --detect --force default
echo "DO NOT WORRY ABOUT THIS CONAN WARNING. It's handled correctly by the script."
case $(uname) in
  Darwin*)
    ;;
  *) conan profile update settings.compiler.libcxx=libstdc++11 default
    ;;
esac
