#!/bin/bash
# FILES=/Users/rajrsingh/workspace/crimeharvest/uk/data/2016-05/*.csv
FILES=./data/2016-05/*.csv

for f in $FILES
do
  echo "Processing $f file..."
  # take action on each file. $f store current file name
  cat $f | couchimport
done