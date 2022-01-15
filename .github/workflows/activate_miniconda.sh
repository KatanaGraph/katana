# This is a bash INCLUDE file. Source it don't run it.

if [ '!' -d "/opt/conda/bin" ]; then
  bash "$HOME/.cache/miniconda/mambaforge.sh" -f -b -p /opt/conda

  # For library compatibility reasons, prefer taking dependencies from
  # higher priority channels even if newer versions exist in lower priority
  # channels.
  /opt/conda/bin/conda config --set channel_priority strict

  # Forces python to be the version we need.
  # TODO(amp): Remove the explicit version.
  /opt/conda/bin/mamba install -n base python=3.8
fi
export PATH="/opt/conda/bin:$PATH"

# Some conda packages have bash activation scripts. Some of those use unbound variable (for example, mkl).
# So disable -u for activation.
UNSET_U=false
if echo "$-" | grep "u"; then
  set +u
  UNSET_U=true
fi

. activate

if $UNSET_U; then
  set -u
fi
