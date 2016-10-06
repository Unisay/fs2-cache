package example

import fs2._
import example.Cache._
import org.scalatest.MustMatchers

class CacheSpec extends org.scalatest.FlatSpec with MustMatchers {

  "Read Through Cache" must "fetch elements it sees for the first time only" in {
    val fetcher = (i: Int) => Task.delay {
      println(s"Fetching $i")
      i.toString
    }
    val ids = List(1, 2, 2, 3, 2)
    val values = Stream(ids: _*).through(inMemory(fetcher))

    values.runLog.unsafeRun() must contain theSameElementsInOrderAs ids.map(_.toString)
  }

}
