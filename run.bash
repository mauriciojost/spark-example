#!/bin/bash

set -e

export PATH=$PATH:$HOME/opt/spark/bin/
mkdir -p /tmp/spark-events
mkdir -p logs

CLASS=eu.pepot.eu.examples.Example
JAR=./target/scala-2.10/spark-example_2.10-0.1.jar
LOG=logs/output.log
TOOK_LOG=logs/took.log
SPARK_DEFAULTS_CONF_FILE=conf/spark-defaults.conf
CONF_FILE=conf/batch.conf

if [ -f data/input/big.csv ] 
then
  echo "### Input file already existent. Skipping generation..."
else
  echo "### Generating input file..."
  mkdir -p data/input
  ./data/scripts/generate-sample-file.py
  echo "### Generating spark events directory..."
  mkdir -p /tmp/spark-events
fi

echo "### Generating artifacts..."
sbt package 

echo "### Preparing configuration file..."

export APP_NAME="${1:-UnnamedApp}"
source $CONF_FILE
cat $SPARK_DEFAULTS_CONF_FILE.template | envsubst > $SPARK_DEFAULTS_CONF_FILE


echo "### Running spark..."
export SPARK_CONF_DIR=`pwd`/conf/
set +e
spark-submit --class $CLASS $JAR &> $LOG
set -e

echo "### Done."

