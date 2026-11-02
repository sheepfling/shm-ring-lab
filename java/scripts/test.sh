#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf build/classes
mkdir -p build/classes
find src/main/java -name '*.java' -print0 | xargs -0 javac --release 17 -Xlint:all -Werror -d build/classes
java -cp build/classes lab.shmring.Main selftest
java -cp build/classes lab.shmring.MappedMain selftest
