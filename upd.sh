#!/bin/bash
set -e

export COMMIT=$(git log --pretty=oneline -n1 | cut -f1 -d' ')
export PROXY_DIR=$(pwd)/../proxy-databutton-com
export ENVFILE=${PROXY_DIR}/data-app-dns-release

echo Previous content of $ENVFILE:
cat $ENVFILE

echo $COMMIT >$ENVFILE

echo New content of $ENVFILE:
cat $ENVFILE

cd $PROXY_DIR
git commit data-app-dns-release -m"Bump data-app-dns-release"
