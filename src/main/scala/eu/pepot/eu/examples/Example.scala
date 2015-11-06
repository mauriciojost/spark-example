package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// LESSONS:
// - Use array types that might end up being primitive light-weight JVM types
// - Avoid conversions as much as possible
// - Avoid groupBy! Use better reduceByKey (to discard elements as soon as possible, decreases execution time by half!!!)
object Example {

  def main(args: Array[String]) {

    val inputDirectory = "localhdfs/input"
    val outputDirectory = "localhdfs/output"

    implicit val sc = new SparkContext(new SparkConf())

    val lines: RDD[String] = sc.textFile(inputDirectory)

    val csvLines = lines.map(getAsCsv)

    val csvLinesFlagged = csvLines.map(csv => (getKey(csv), csv)).reduceByKey(getLatestCsvLine).map { case (key, csvLine) => ("Y" ++ csvLine).mkString(",") }

    csvLinesFlagged.saveAsTextFile(outputDirectory)

  }

  def getAsCsv(lineString: String) = lineString.split(",")
  def getKey(csv: Array[String]) = csv(0)
  def getEventNumber(csv: Array[String]) = csv(1)
  def getLatestCsvLine(csvLine1: Array[String], csvLine2: Array[String]) = if (getEventNumber(csvLine1) > getEventNumber(csvLine2)) csvLine1 else csvLine2


}

