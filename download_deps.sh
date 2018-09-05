#!/bin/bash

rm -rf ./dep/
mkdir -p dep/
for i in `cat requirements.txt`;
do
  echo "Downloading dep: $i ..."
  pip2 install --install-option="--prefix=" $i -t ./dep/
done
