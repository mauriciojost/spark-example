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

  case class LightEvent(id: Int, version: Int)

  case class Event(id: Int, version: Int, payload: String)

  case class FlaggedLightEvent(id: Int, version: Int, latest: Boolean)

  case class FlaggedEvent(id: Int, version: Int, latest: Boolean, payload: String)

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
      .collectAsMap()

    val flagsBroadcasted = sc.broadcast(flags)

    val allCsvLinesFlagged = events
      .map { event =>
        val idAndVersionHash = LightEvent(event.id, event.version)
        val flag = flagsBroadcasted.value(idAndVersionHash)
        FlaggedEvent(event.id, event.version, flag, event.payload)
      }

    allCsvLinesFlagged.map(flaggedEventToCsv).saveAsTextFile(outputDirectory)


  }

  def flagEvents(lightEventsWithSameId: Iterable[LightEvent]): Iterable[(LightEvent, Boolean)] = {
    val newestEventWithinTheGroup = lightEventsWithSameId.map(_.version).max
    val flagsMap = lightEventsWithSameId.map { csvLine =>
      if (csvLine.version == newestEventWithinTheGroup) {
        (csvLine, true)
      } else {
        (csvLine, false)
      }
    }
    flagsMap
  }


  def transformLineToEvent(lineString: String): Event = {
    lineString.split(",") match {
      case Array(id, version, payload) => Event(id.toInt, version.toInt, payload)
    }
  }

  def getEventId(csv: Array[String]): String = csv(0)

  def getEventVersion(csv: CsvLine): String = csv(1)

  def getLightEvent(event: Event): LightEvent = LightEvent(id = event.id, version = event.version)

  def flaggedEventToCsv(fEvent: FlaggedEvent): String =
    fEvent.id + "," + fEvent.version + "," + fEvent.latest + "," + fEvent.payload

}

