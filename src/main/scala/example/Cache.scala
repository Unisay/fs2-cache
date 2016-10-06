package example

import fs2._
import fs2.util._
import fs2.util.syntax._
import scala.language.higherKinds

/**

  I need to integrate read-through cache in the fs2 Stream.
  Please consider the details:
  ```scala

  // given a key returns a task of value (fetcher)
  def fetcher(key: Int): Task[String] = Task.delay {
    println(s"Fetching a value for key $key")
    key.toString
  }

  // A stream of keys
  val keys: Stream[Task, Int] = Stream(1, 2, 2, 3, 2)

  // A cache
  val cache: Map[Int, String] = Map.empty

  // A stream of values taken from cache or fetcher
  val values: Stream[Task, String] = keys.(some steps that use fetcher?)

  // Must print only:
  // Fetching a value for key 1
  // Fetching a value for key 2
  // Fetching a value for key 3
  values.runLog.unsafeRun mustEqual Vector("1", "2", "2", "3", "2")

  ```

  */

object Cache {

  def mapAccumulateEval[F[_]: Monad,S,I,O](init: S)(f: (S,I) => F[(S,O)]): Pipe[F,I,(S,O)] =
  _.pull { handle =>
    handle.receive { case (chunk, h) =>
      val f2: (S, I) => F[(S, (S, O))] = (s: S, i: I) => {
        f(s, i) map { case (newS, newO) => (newS, (newS, newO)) }
      }
      val eval: Pull[F, (S, O), (S, Chunk[(S, O)])] = Pull.eval(chunkMapAccumulateEval(chunk)(init)(f2))
      eval.flatMap { case (s, _) => _mapAccumulateEval0(s)(f2)(h) }
    }
  }

  private def _mapAccumulateEval0[F[_]: Monad,S,I,O](init: S)
                                                    (f: (S,I) => F[(S,(S,O))])
                                                    (handle: Handle[F,I]): Pull[F,(S,O),Handle[F,I]] =
    handle.receive { case (chunk, h) =>
      Pull.eval(chunkMapAccumulateEval(chunk)(init)(f)).flatMap {
        case (s, c) =>
          // Pull.output(c) >> ???
          _mapAccumulateEval0(s)(f)(h)
      }
    }

  /** Simultaneously folds and maps this chunk, returning the output of the fold and the transformed chunk. */
  def chunkMapAccumulateEval[F[_]: Monad, S,B,A](chunk: Chunk[A])(s0: S)(f: (S,A) => F[(S,B)]): F[(S,Chunk[B])] = {
    type BUF = collection.mutable.ArrayBuffer[B]
    val buf = new collection.mutable.ArrayBuffer[B](chunk.size)
    chunk.foldLeft(implicitly[Monad[F]].pure((s0, buf))) { (acc: F[(S, BUF)], a: A) =>
      for {
        t1 <- acc
        (s, bf) = t1
        t2 <- f(s, a)
        (s1, b) = t2
      } yield (s1, bf += b)
    } map {
      case (s, bf) => (s, Chunk.indexedSeq(bf))
    }
  }

  def inMemory[K, V](fetch: K => Task[V]): Pipe[Task, K, V] = _
    .through {
      mapAccumulateEval(Map.empty[K, V]) { (s: Map[K, V], k: K) =>
          s.get(k)
          .map(v => Task.now((s, v)))
          .getOrElse(fetch(k).map(value => (s.updated(k, value), value)))
        }
      }
    .map { case (_, v) => v }
}
