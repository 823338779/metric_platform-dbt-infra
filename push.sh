#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

for repo in "$repo_root/vendor/dbt" "$repo_root/vendor/metricflow" "$repo_root"; do
    branch="$(git -C "$repo" symbolic-ref --quiet --short HEAD)"
    printf 'Force pushing %s (%s)\n' "$repo" "$branch"
    git -C "$repo" push --force origin "HEAD:refs/heads/$branch"
done
