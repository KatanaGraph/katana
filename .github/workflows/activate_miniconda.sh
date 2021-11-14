# This is a bash INCLUDE file. Source it don't run it.

if [ '!' -d "$HOME/miniconda/bin" ]; then
  bash "$HOME/.cache/miniconda/mambaforge.sh" -f -b -p "$HOME/miniconda"
fi
export PATH="$HOME/miniconda/bin:$PATH"

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
