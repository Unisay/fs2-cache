package example

import fs2._
import fs2.util.{Functor, Monad}

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

  I tried to use `mapAccumulate(cache)(...)` but it works only with fetchers that return raw value (String) not Task[String]
  Looks like what I need should be named `flatMapAccumulate`

  Please advise
  */

object Cache {

  def mapAccumulateEval[F[_]: Monad,S,I,O](init: S)(f: (S,I) => F[(S,O)]): Pipe[F,I,F[(S,O)]] =
  _.pull { handle =>
    handle.receive { case (chunk, h) =>
      val monad: Monad[F] = implicitly[Monad[F]]
      val f2 = (s: S, i: I) => {
        monad.map(f(s, i)) {
          case (newS, newO) => (newS, (newS, newO))
        }
      }
      monad.flatMap(chunkMapAccumulateEval(chunk)(init)(f2)) {
        case (s, o) => Pull.output(o) >> _mapAccumulateEval0(s)(f2)(h)
      }
    }
  }

  private def _mapAccumulateEval0[F[_],S,I,O](init: S)(f: (S,I) => F[(S,(S,O))]): Handle[F,I] => Pull[F,(S,O),Handle[F,I]] =
    _.receive { case (chunk, h) =>
      val (s, o) = chunkMapAccumulateEval(chunk)(init)(f)
      Pull.output(o) >> _mapAccumulateEval0(s)(f)(h)
    }

  /** Simultaneously folds and maps this chunk, returning the output of the fold and the transformed chunk. */
  def chunkMapAccumulateEval[F[_]: Monad, S,B,A](chunk: Chunk[A])(s0: S)(f: (S,A) => F[(S,B)]): F[(S,Chunk[B])] = {
    val buf = new collection.mutable.ArrayBuffer[B](size)
    var s = s0
    for { c <- chunk.iterator } {
      val (newS, newC) = f(s, c)
      buf += newC
      s = newS
    }
    (s, Chunk.indexedSeq(buf))
  }

  def inMemory[K, V](fetch: K => Task[V]): Pipe[Task, K, V] = _
    .through {
      pipe.mapAccumulateEval(Map.empty[K, V]) { (s: Map[K, V], k: K) =>
          s.get(k)
          .map(v => Task.now((state, v)))
          .getOrElse(fetch(k).map(value => (state.updated(k, value), value)))
        }
      }

}
