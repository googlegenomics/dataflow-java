#!/bin/bash

# Main script for integration tests. This should call every test we want
# to run before cutting a new release.

# This file contains site-specific configuration and thus isn't checked in.
source setup-variables

# exit the script if any command fails
set -o errexit

# Run the tests
for i in src/main/scripts/test_*.sh; do
  $i
done
