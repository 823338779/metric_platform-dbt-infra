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

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "bootstrap.sh supports macOS only." >&2
  exit 1
fi

uv_is_supported() {
  local version
  version="$(uv --version 2>/dev/null)" || return 1
  [[ "$version" =~ ^uv\ (0\.(1[2-9]|[2-9][0-9])\.|[1-9][0-9]*\.) ]]
}

if [[ "$check_only" == true ]]; then
  command -v git >/dev/null 2>&1 || {
    echo "Git is missing. Run ./bootstrap.sh without --check first." >&2
    exit 1
  }
  command -v uv >/dev/null 2>&1 || {
    echo "uv is missing. Run ./bootstrap.sh without --check first." >&2
    exit 1
  }
  uv_is_supported || {
    echo "uv 0.12 or newer is required. Run ./bootstrap.sh without --check first." >&2
    exit 1
  }
  case "$(uv tool list)" in
    *"hatch v1.18.1"*) ;;
    *)
      echo "Hatch 1.18.1 is missing. Run ./bootstrap.sh without --check first." >&2
      exit 1
      ;;
  esac
  python_path="$(uv --no-python-downloads python find --system --managed-python 3.12)" || {
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
if ! uv_is_supported; then
  brew upgrade uv
fi
uv_is_supported || {
  echo "uv 0.12 or newer is required after installation." >&2
  exit 1
}
uv python install --upgrade 3.12
uv tool install --force "hatch==1.18.1"
exec uv run --no-project --python 3.12 "$repository_root/scripts/bootstrap.py"
