#!/bin/bash

CLASS=eu.pepot.eu.examples.Example
JAR=./target/scala-2.10/spark-example_2.10-0.1.jar

if [ -f localhdfs/input/big.csv ] 
then
  echo "### Input file already existent. Skipping generation..."
else
  echo "### Generating input file..."
  mkdir -p localhdfs/input
  ./localhdfs/scripts/generate-sample-file.sh
fi

echo "### Generating artifacts..."
sbt package 

echo "### Ready to run (see conf directory first):"
echo "       export SPARK_CONF_DIR=`pwd`/conf/"
echo "       spark-submit --class $CLASS $JAR"

