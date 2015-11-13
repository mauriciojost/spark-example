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

  type CsvLine = Array[String]

  case class LightEvent(id: String, version: String)
  case class Event(id: String, version: String, payload: String)
  case class FlaggedLightEvent(id: String, version: String, latest: String)
  case class FlaggedEvent(id: String, version: String, latest: String, payload: String)

  def main(args: Array[String]) {

    val inputDirectory = "data/input"
    val outputDirectory = "data/output"

    implicit val sc = new SparkContext(new SparkConf())

    val rawLines: RDD[String] = sc.textFile(inputDirectory)

    val events = rawLines.map(transformLineToEvent)

    val flags = events
      .map(getLightEvent)
      .groupBy(_.id)
      .flatMap { case (eventId, eventsWithSameId) => flagEvents(eventsWithSameId) }
      .distinct()

    val allCsvLinesFlagged = events
      .map(event => (generateEventIdAndVersionHash(LightEvent(event.id, event.version)), event))
      .join(flags)
      .map { case (eventIdAndVersion, (event, flag)) => FlaggedEvent(event.id, event.version, flag, event.payload) }
    allCsvLinesFlagged.map(flaggedEventToCsv).saveAsTextFile(outputDirectory)


  }

  def flagEvents(csvLinesWithSameKey: Iterable[LightEvent]): Iterable[(String, String)] = {
    val newestEventWithinTheGroup = csvLinesWithSameKey.map(_.version).max
    val flaggedCsvLines = csvLinesWithSameKey.map { csvLine =>
      if (csvLine.version == newestEventWithinTheGroup) {
        (generateEventIdAndVersionHash(csvLine), "Y")
      } else {
        (generateEventIdAndVersionHash(csvLine), "N")
      }
    }
    flaggedCsvLines
  }


  def transformLineToEvent(lineString: String): Event = {
    lineString.split(",") match {
      case Array(id, version, payload) => Event(id, version, payload)
    }
  }

  def getEventId(csv: Array[String]): String = csv(0)

  def getEventVersion(csv: CsvLine): String = csv(1)

  def getLightEvent(event: Event): LightEvent = LightEvent(id = event.id, version = event.version)

  def generateEventIdAndVersionHash(csvLine: LightEvent): String = csvLine.id + "," + csvLine.version

  def flaggedEventToCsv(fEvent: FlaggedEvent): String =
    fEvent.id + "," + fEvent.version + "," + fEvent.latest + "," + fEvent.payload

}

