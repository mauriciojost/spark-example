package eu.pepot.eu.examples

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Helper._
import CommonTypes._

object Flagger {

  def flagEventsRDD(events: RDD[Event])(implicit sc: SparkContext): RDD[FlaggedEvent] = {
    val flags = events
      .map(event => (event.id, getLightEventFromEvent(event)))
      .reduceByKey(getNewerEvent)
      .flatMap { case (eventId, eventsWithSameId) => flagGreaterEventInGroup(List(eventsWithSameId)) }
      .distinct()
      .collectAsMap()

    val flagsBroadcasted = sc.broadcast(flags)

    val allEventsFlagged = events
      .flatMap { event =>
      val idAndVersionHash = LightEvent(event.id, event.version)
      flagsBroadcasted.value.get(idAndVersionHash).map { f =>
        FlaggedEvent(event.id, event.version, f, event.payload)
      }
    }
    allEventsFlagged
  }

  def flagGreaterEventInGroup(lightEventsWithSameId: Iterable[LightEvent]): Iterable[(LightEvent, Boolean)] = {
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

}

