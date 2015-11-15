#!/bin/bash

export PATH=$PATH:$HOME/opt/spark/bin/
mkdir -p /tmp/spark-events
mkdir -p logs

CLASS=eu.pepot.eu.examples.Example
JAR=./target/scala-2.10/spark-example_2.10-0.1.jar
LOG=logs/output.log
TOOK_LOG=logs/took.log

if [ -f data/input/big.csv ] 
then
  echo "### Input file already existent. Skipping generation..."
else
  echo "### Generating input file..."
  mkdir -p data/input
  ./data/scripts/generate-sample-file.bash
  echo "### Generating spark events directory..."
  mkdir -p /tmp/spark-events
fi

echo "### Generating artifacts..."
sbt package 

echo "### Running spark..."
export SPARK_CONF_DIR=`pwd`/conf/
spark-submit --class $CLASS $JAR &> $LOG
cat $LOG | grep took > $TOOK_LOG

echo "### Done."

