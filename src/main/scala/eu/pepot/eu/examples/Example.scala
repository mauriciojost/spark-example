package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  type CsvLine = Array[String]

  case class LightEvent(id: Long, version: Int)

  case class Event(id: Long, version: Int, payload: String)

  case class FlaggedLightEvent(id: Long, version: Int, latest: Boolean)

  case class FlaggedEvent(id: Long, version: Int, latest: Boolean, payload: String)

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

    val allEventsFlagged = events
      .map { event =>
        val idAndVersionHash = LightEvent(event.id, event.version)
        val flag = flagsBroadcasted.value(idAndVersionHash)
        FlaggedEvent(event.id, event.version, flag, event.payload)
      }

    allEventsFlagged.map(flaggedEventToCsv).saveAsTextFile(outputDirectory)


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
      case Array(id, version, payload) => Event(id.toLong, version.toInt, payload)
    }
  }

  def getEventId(csv: Array[String]): String = csv(0)

  def getEventVersion(csv: CsvLine): String = csv(1)

  def getLightEvent(event: Event): LightEvent = LightEvent(id = event.id, version = event.version)

  def flaggedEventToCsv(fEvent: FlaggedEvent): String =
    fEvent.id + "," + fEvent.version + "," + fEvent.latest + "," + fEvent.payload

}

