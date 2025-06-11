#!/bin/bash
set -e

# Defaults if not set
: "${ARRIVAL_COUNT:=1}"
: "${DURATION:=60}"

# Expand template with env vars
envsubst < load-test.template.yaml > load-test.yaml

# Run the test
run run load-test.yaml
