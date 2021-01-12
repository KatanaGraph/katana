#!/bin/bash
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

TARGET_DIR="$TMP_DIR/$(hostname)_logs"

mkdir -p "$TARGET_DIR"

function dump() {
  NAME=$1
  echo -n "Captureing information: $NAME ..."
  cat > $TMP_DIR/$NAME.sh
  bash -x $TMP_DIR/$NAME.sh > "$TARGET_DIR/$NAME.log" 2>&1
  echo "Done."
}

cp -a $0 "$TARGET_DIR"

dump provenance <<EOF
uname -a
date
lsb_release -a
pwd
EOF

dump apt_packages <<EOF
apt list --installed
EOF

dump pip_packages <<EOF
which python
python --version
pip list --format=columns
which python3
python3 --version
pip3 list --format=columns
EOF

dump conda_packages <<EOF
echo $CONDA_PREFIX
echo $CONDA_EXE
which conda
$CONDA_EXE info || conda info
$CONDA_EXE list || conda list
EOF

dump filesystem <<"EOF"
(
# List files listed in well known root fs directories
find /opt /usr /lib* '!' -path "/usr/src/*" '!' -path "*/lib/modules/*" '!' -path "*/share/*"
# List files listed in notable PATH variables
find $(echo "$LD_LIBRARY_PATH" "$LIBRARY_PATH" "$INCLUDE_PATH" "$C_INCLUDE_PATH" "$PATH" | sed -ne 's/:/ /gp')
# List files listed in gcc built in search dirs
find $(gcc -print-search-dirs | sed -ne 's/.*=//; \@^/@s/:/ /gp')
) | sort -u
EOF

dump ldso_config <<EOF
echo "$LD_LIBRARY_PATH"
tail -n +1 /etc/ld.so.conf /etc/ld.so.conf.d/*
EOF

dump gcc_config <<EOF
which gcc
gcc -dumpfullversion
gcc -print-search-dirs
gcc -print-sysroot
gcc -print-sysroot-headers-suffix
EOF

dump cmake_config <<EOF
which cmake
cmake --version
EOF

dump env <<EOF
env
EOF

OUTPUT_FILENAME="$(hostname)_env_info.tar.gz"
tar czf "$OUTPUT_FILENAME" -C $TMP_DIR "$(hostname)_logs"

echo
echo "Generated environment information archive: $OUTPUT_FILENAME"
echo "Please send the archive to your katana contact with any information"
echo "you have about your problem to help us debug it."
