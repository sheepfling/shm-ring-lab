#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-/build}"

printf '%s\n' 'Linux container details:'
cat /etc/os-release
uname -a
c++ --version | head -n 1
cmake --version | head -n 1

printf '\n%s\n' 'Running bounded correctness suite three times:'
ctest --test-dir "$build_dir" --repeat until-fail:3 --output-on-failure

printf '\n%s\n' 'Running separate-process count/checksum proof:'
python3 /src/scripts/linux_process_proof.py "$build_dir/shm_ring_demo" --messages 5000
