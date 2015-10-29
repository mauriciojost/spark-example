#!/bin/bash

CLASS=eu.pepot.eu.examples.Example
JAR=./target/scala-2.10/spark-example_2.10-0.1.jar

mkdir -p localhdfs/input

sbt package 

spark-submit  --master local[4] --executor-memory 1G --class $CLASS $JAR localhdfs/input localhdfs/output

cat localhdfs/output/*part*
