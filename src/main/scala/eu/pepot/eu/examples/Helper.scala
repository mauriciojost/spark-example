package eu.pepot.eu.examples

object Helper {

  type CsvLine = Array[String]

  case class LightEvent(id: Long, version: Int)

  case class Event(id: Long, version: Int, payload: String)

  case class FlaggedLightEvent(id: Long, version: Int, latest: Boolean)

  case class FlaggedEvent(id: Long, version: Int, latest: Boolean, payload: String)


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


}
