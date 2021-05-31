#!/bin/sh
set -e

echo "Testing with pytest..."
pytest -s -v python/test
