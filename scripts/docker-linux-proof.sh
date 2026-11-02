#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${SHM_RING_LAB_LINUX_IMAGE:-shm-ring-lab-linux-proof:bookworm}"

docker info --format 'Docker {{.ServerVersion}} ({{.OSType}}/{{.Architecture}})'
docker build --file "$repo_root/docker/linux/Dockerfile" --tag "$image" "$repo_root"
docker run --rm "$image"
