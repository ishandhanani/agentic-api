#!/usr/bin/env bash
set -euo pipefail

requested_version="${AGENTIC_API_RELEASE_VERSION:-}"
workspace_version="$(awk '
  $0 == "[workspace.package]" { in_section = 1; next }
  in_section && /^\[/ { in_section = 0 }
  in_section && $1 == "version" { gsub(/"/, "", $3); print $3; exit }
' Cargo.toml)"

if [[ -z "$workspace_version" ]]; then
  echo "unable to determine the Cargo workspace version" >&2
  exit 1
fi

if [[ "$requested_version" != "$workspace_version" ]]; then
  echo "requested release version ${requested_version} does not match workspace version ${workspace_version}" >&2
  exit 1
fi
