package eu.pepot.eu.examples

import com.holdenkarau.spark.testing.{RDDComparisions, SharedSparkContext}
import eu.pepot.eu.examples.CommonTypes._
import org.scalatest.FunSuite

class ExampleSpec extends FunSuite with SharedSparkContext {

  test("simple flagging") {
    val events = List(
      Event(1999, 0, ""),
      Event(1999, 1, ""),
      Event(2000, 2, ""),
      Event(2000, 3, "")
    )
    val expected = List(
      FlaggedEvent(1999, 1, true, ""),
      FlaggedEvent(2000, 3, true, "")
    )

    assert(None ===
      RDDComparisions.compare(sc.parallelize(expected), Flagger.flagEventsRDD(sc.parallelize(events))(sc)))
  }

}

