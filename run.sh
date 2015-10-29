#!/bin/bash

mkdir -p localhdfs/input
cp README.md localhdfs/input

spark-submit --master local[2] --class eu.pepot.eu.examples.Example ./target/scala-2.10/spark-example_2.10-0.1.jar localhdfs/input localhdfs/output

