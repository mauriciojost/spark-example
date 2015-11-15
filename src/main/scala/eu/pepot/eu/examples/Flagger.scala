package eu.pepot.eu.examples

import eu.pepot.eu.examples.Helper._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Flagger {

  def flagEventsRDD(events: RDD[Event]) (implicit sc: SparkContext): RDD[FlaggedEvent] = {
    val flags = events
      .map(getLightEventFromEvent)
      .groupBy(_.id)
      .flatMap { case (eventId, eventsWithSameId) => flagGreaterEventInGroup(eventsWithSameId) }
      .distinct()
      .collectAsMap()

    val flagsBroadcasted = sc.broadcast(flags)

    val allEventsFlagged = events
      .map { event =>
      val idAndVersionHash = LightEvent(event.id, event.version)
      val flag = flagsBroadcasted.value(idAndVersionHash)
      FlaggedEvent(event.id, event.version, flag, event.payload)
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

