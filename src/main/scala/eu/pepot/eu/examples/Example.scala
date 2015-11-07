package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// LESSONS:
// - Use array types that might end up being primitive light-weight JVM types
// - Avoid conversions as much as possible
// - Avoid groupBy! Use better reduceByKey (to discard elements as soon as possible, decreases execution time by half!!!)
// - Think about repartitioning to avoid overhead of data transfer when having too many partitions on distant executors (reduces 1/7 the time)
// - Think better about coalescing with the amount of workers, it can only decrease the amount of partitions with less data transfering than a full shuffle
//   and speds up processing (decreases execution time to one third of the original one!!!!)
// - Coalescing (4 threads in local master):
//    NO  ->  47s
//     4  ->  26s
//     8  ->  26s
//    16  ->  28s
//    32  ->  31s
//    64  ->  38s
//   128  ->  46s


object Example {

  def main(args: Array[String]) {

    val inputDirectory = "localhdfs/input"
    val outputDirectory = "localhdfs/output"

    implicit val sc = new SparkContext(new SparkConf())

    val lines: RDD[String] = sc.textFile(inputDirectory)

    val csvLines = lines.map(getAsCsv)

    val csvLinesFlagged = csvLines.keyBy(getKey).reduceByKey(getLatestCsvLine).values.map(csv => ("Y" ++ csv).mkString(","))

    csvLinesFlagged.saveAsTextFile(outputDirectory)

  }

  def getAsCsv(lineString: String) = lineString.split(",")
  def getKey(csv: Array[String]) = csv(0)
  def getEventNumber(csv: Array[String]) = csv(1)
  def getLatestCsvLine(csvLine1: Array[String], csvLine2: Array[String]) = if (getEventNumber(csvLine1) > getEventNumber(csvLine2)) csvLine1 else csvLine2


}

