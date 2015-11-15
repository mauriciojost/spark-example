package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import Constants._
import Helper._
import Flagger._
import CommonTypes._

object Example {

  def main(args: Array[String]) {

    implicit val sc = new SparkContext(new SparkConf())

    val rawLines: RDD[String] = sc.textFile(InputDirectory)

    val events = rawLines.map(transformTextLineToEvent)

    val allEventsFlagged: RDD[FlaggedEvent] = flagEventsRDD(events)

    println("Result: " + allEventsFlagged.toDebugString)

    allEventsFlagged.map(transformFlaggedEventIntoCsvLine).saveAsTextFile(OutputDirectory)

  }

}

