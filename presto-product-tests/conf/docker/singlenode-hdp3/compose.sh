#!/usr/bin/env bash

set -euo pipefail

source "${BASH_SOURCE%/*}/../common/compose-commons.sh"
source "${BASH_SOURCE%/*}/../../../conf/product-tests-config-hdp3.sh"
docker-compose \
    -f "${BASH_SOURCE%/*}/../common/standard.yml" \
    -f "${BASH_SOURCE%/*}/docker-compose.yml" \
    "$@"
