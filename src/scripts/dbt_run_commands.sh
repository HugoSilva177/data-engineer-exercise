#!/usr/bin/env bash

echo "Checking for dbt commands to run..."

if [ $# -eq 0 ]
then
  echo "There is no commands to run..."
  echo "Finishing the script!"
else
#  bash -c "dbt seed"
  for var in "$@"
  do
    echo "Running command: '$var'"
    bash -c "$var"
  done
fi
