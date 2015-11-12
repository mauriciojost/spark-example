#!/bin/bash

export PATH=$PATH:$HOME/opt/spark/bin/
mkdir -p /tmp/spark-events

CLASS=eu.pepot.eu.examples.Example
JAR=./target/scala-2.10/spark-example_2.10-0.1.jar
LOG=execution.log

if [ -f localhdfs/input/big.csv ] 
then
  echo "### Input file already existent. Skipping generation..."
else
  echo "### Generating input file..."
  mkdir -p localhdfs/input
  ./localhdfs/scripts/generate-sample-file.sh
  echo "### Generating spark events directory..."
  mkdir -p /tmp/spark-events
fi

echo "### Generating artifacts..."
sbt package 

echo "### Running spark..."
export CURRENT_COMMIT=`git rev-parse HEAD`
export SPARK_CONF_DIR=`pwd`/conf/
spark-submit --class $CLASS $JAR &> $LOG
echo $CURRENT_COMMIT >> $LOG

echo "### Done."

