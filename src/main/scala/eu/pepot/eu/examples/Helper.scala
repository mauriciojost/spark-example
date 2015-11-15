package eu.pepot.eu.examples

import eu.pepot.eu.examples.CommonTypes._

object Helper {

  def getEventId(csv: Array[String]): String = csv(0)

  def getEventVersion(csv: CsvLine): String = csv(1)

  def getLightEventFromEvent(event: Event): LightEvent = {
    LightEvent(id = event.id, version = event.version)
  }

  def transformFlaggedEventIntoCsvLine(fEvent: FlaggedEvent): String = {
    fEvent.id + "," + fEvent.version + "," + fEvent.latest + "," + fEvent.payload
  }

  def transformTextLineToEvent(lineString: String): Event = {
    lineString.split(",") match {
      case Array(id, version, payload) => Event(id.toLong, version.toInt, payload)
    }
  }

  def getNewerEvent(eventA: LightEvent, eventB: LightEvent): LightEvent = {
    if (eventA.id > eventB.id)
      eventA
    else
      eventB
  }


}
