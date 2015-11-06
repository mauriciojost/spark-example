package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// LESSONS:
// - Use array types that might end up being primitive light-weight JVM types
// - Avoid conversions as much as possible
object Example {

  def main(args: Array[String]) {

    val inputDirectory = "localhdfs/input"
    val outputDirectory = "localhdfs/output"

    implicit val sc = new SparkContext(new SparkConf())

    val lines: RDD[String] = sc.textFile(inputDirectory)

    val csvLines = lines.map(getAsCsv)

    val csvLinesGrouped = csvLines.groupBy(getKey)

    val csvLinesFlagged: RDD[String] = csvLinesGrouped.flatMap { case (keyOfGroup, csvLinesWithSameKey) =>

      val newestEventWithinTheGroup = csvLinesWithSameKey.map(getEventNumber).max

      val flaggedCsvLine = csvLinesWithSameKey.map { csvLine =>
        if (getEventNumber(csvLine) == newestEventWithinTheGroup) {
          ("Y" ++ csvLine).mkString(",")
        } else {
          ("N" ++ csvLine).mkString(",")
        }
      }

      flaggedCsvLine

    }

    csvLinesFlagged.saveAsTextFile(outputDirectory)

  }

  def getAsCsv(lineString : String) = lineString.split(",")
  def getKey(csv : Array[String]) = csv(0)
  def getEventNumber(csv : Array[String]) = csv(1)

}

