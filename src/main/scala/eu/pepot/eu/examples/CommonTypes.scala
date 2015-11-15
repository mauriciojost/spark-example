package eu.pepot.eu.examples

object CommonTypes {

  type CsvLine = Array[String]

  case class LightEvent(id: Long, version: Int)

  case class Event(id: Long, version: Int, payload: String)

  case class FlaggedLightEvent(id: Long, version: Int, latest: Boolean)

  case class FlaggedEvent(id: Long, version: Int, latest: Boolean, payload: String)


}
