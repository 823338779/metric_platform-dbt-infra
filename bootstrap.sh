#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check_only=false

case "$#" in
  0)
    ;;
  1)
    if [[ "$1" != "--check" ]]; then
      echo "Usage: ./bootstrap.sh [--check]" >&2
      exit 2
    fi
    check_only=true
    ;;
  *)
    echo "Usage: ./bootstrap.sh [--check]" >&2
    exit 2
    ;;
esac

if [[ "$check_only" == true ]]; then
  command -v git >/dev/null 2>&1 || {
    echo "Git is missing. Run ./bootstrap.sh without --check first." >&2
    exit 1
  }
  command -v uv >/dev/null 2>&1 || {
    echo "uv is missing. Run ./bootstrap.sh without --check first." >&2
    exit 1
  }
  python_path="$(uv --no-python-downloads python find 3.12)" || {
    echo "uv-managed Python 3.12 is missing. Run ./bootstrap.sh without --check first." >&2
    exit 1
  }
  exec "$python_path" "$repository_root/scripts/bootstrap.py" --check
fi

if ! command -v brew >/dev/null 2>&1; then
  NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  if [[ -x /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
  elif [[ -x /usr/local/bin/brew ]]; then
    eval "$(/usr/local/bin/brew shellenv)"
  else
    echo "Homebrew installation completed but brew could not be resolved." >&2
    exit 1
  fi
fi

brew install git uv
uv python install 3.12
uv tool install "hatch==1.18.1"
exec uv run --no-project --python 3.12 "$repository_root/scripts/bootstrap.py"
