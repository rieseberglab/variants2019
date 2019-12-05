#!/bin/bash

set -exo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
(
    cd "$HERE/.."
    python -m variants wgs_all/samples.json --reference "ha412" "$@"
)
