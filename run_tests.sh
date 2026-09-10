#!/bin/bash
# Run tests in both Python 2 and Python 3

set -e

echo "============================================"
echo "Running tests with Python 2"
echo "============================================"
python2 -m pytest test_tinychat.py -v

echo ""
echo "============================================"
echo "Running tests with Python 3"
echo "============================================"
python3 -m pytest test_tinychat.py -v

echo ""
echo "============================================"
echo "All tests passed in both Python 2 and 3!"
echo "============================================"