#!/bin/sh
set -e

echo "Testing with pytest..."
pytest -v python/test
