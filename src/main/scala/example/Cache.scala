package example

import fs2._
import fs2.util._
import fs2.util.syntax._
import scala.collection.mutable.ArrayBuffer
import scala.language.higherKinds

object Cache {

  def mapAccumulateEval[F[_]: Monad,S,I,O](init: S)(f: (S,I) => F[(S,O)]): Pipe[F,I,(S,O)] =
  _.pull { handle =>
    handle.receive { case (chunk, h) =>
      val f2: (S, I) => F[(S, (S, O))] = (s: S, i: I) => {
        f(s, i) map { case t @ (newS, newO) => (newS, t) }
      }
      Pull.eval(chunkMapAccumulateEval(chunk)(init)(f2))
        .flatMap { case (s, c) => Pull.output(c) >> _mapAccumulateEval0(s)(f2)(h) }
    }
  }

  private def _mapAccumulateEval0[F[_]: Monad,S,I,O](init: S)
                                                    (f: (S,I) => F[(S,(S,O))])
                                                    (handle: Handle[F,I]): Pull[F,(S,O),Handle[F,I]] =
    handle.receive { case (chunk, h) =>
      Pull.eval(chunkMapAccumulateEval(chunk)(init)(f))
        .flatMap { case (s, c) => Pull.output(c) >> _mapAccumulateEval0(s)(f)(h) }
    }

  def chunkMapAccumulateEval[F[_]: Monad, S,B,A](chunk: Chunk[A])(s0: S)(f: (S,A) => F[(S,B)]): F[(S,Chunk[B])] = {
    chunk.foldLeft(implicitly[Monad[F]].pure((s0, new ArrayBuffer[B](chunk.size)))) { (acc, a: A) =>
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

  def readThroughMap[K, V](fetch: K => Task[V]): Pipe[Task, K, V] = _
    .through {
      mapAccumulateEval(Map.empty[K, V]) { (s, k) =>
        s get k map (v => Task.now((s, v))) getOrElse fetch(k).map(value => (s.updated(k, value), value))
      }
    }
    .map { case (_, v) => v }
}
