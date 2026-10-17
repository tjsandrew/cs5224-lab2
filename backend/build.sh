#!/bin/sh
# Package the Lambda: app.py and the page it serves, in one zip for upload.
set -e
cd "$(dirname "$0")"
mkdir -p dist
rm -f dist/lambda.zip
zip -q dist/lambda.zip app.py index.html
echo "Created $(pwd)/dist/lambda.zip"
