package example

import fs2._
import org.scalatest.MustMatchers
import scala.collection.mutable.ListBuffer

class CacheSpec extends org.scalatest.FlatSpec with MustMatchers {

  "Read Through Cache" must "fetch elements it sees for the first time only" in {
    val effects = ListBuffer[Int]()
    val fetcher = (i: Int) => Task.delay { effects += i; i.toString }
    val ids = List(1, 2, 2, 3, 2)

    val results = Stream(ids: _*).through(Cache.readThroughMap(fetcher)).runLog.unsafeRun()

    effects must contain theSameElementsInOrderAs List(1, 2, 3)
    results must contain theSameElementsInOrderAs List("1", "2", "2", "3", "2")
  }

}
