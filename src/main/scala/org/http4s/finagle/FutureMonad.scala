package org.http4s.finagle

import scala.languageFeature.{ higherKinds, reflectiveCalls }

import com.twitter.util._

import scalaz._
import scalaz.syntax.either._

trait FutureMonad extends MonadError[({ type λ[E, α] = Future[α] })#λ, Throwable]
    with MonadPlus[Future] with Monad[Future] with Cobind[Future]
    with Nondeterminism[Future] with Zip[Future] with Catchable[Future] {
  import Future._

  override def raiseError[A](e: Throwable): Future[A] =
    exception(e)

  override def handleError[A](fa: Future[A])(f: Throwable => Future[A]): Future[A] =
    fa rescue { case t => f(t) }

  override def bind[A, B](fa: Future[A])(f: A => Future[B]): Future[B] =
    fa flatMap f

  override def point[A](a: => A): Future[A] =
    Future(a)

  override def map[A, B](fa: Future[A])(f: A => B): Future[B] =
    fa map f

  override def ap[A, B](fa: => Future[A])(f: => Future[A => B]): Future[B] =
    (f join fa) map { case (f1, a1) => f1(a1) }

  override def cobind[A, B](fa: Future[A])(f: Future[A] => B): Future[B] =
    Future(f(fa))

  override def empty[A]: Future[A] =
    raiseError(new Try.PredicateDoesNotObtain)

  override def plus[A](fa: Future[A], fb: => Future[A]): Future[A] =
    fa rescue { case _ => fb }

  override def filter[A](fa: Future[A])(f: A => Boolean): Future[A] =
    fa filter f

  override def chooseAny[A](head: Future[A], tail: Seq[Future[A]]): Future[(A, Seq[Future[A]])] =
    select(head +: tail) flatMap {
      case (either, residuals) => const(either) map { (_, residuals) }
    }

  override def gather[A](fs: Seq[Future[A]]): Future[List[A]] =
    collect(fs) map { _.toList }

  override def zip[A, B](a: => Future[A], b: => Future[B]): Future[(A, B)] =
    a join b

  override def attempt[A](fa: Future[A]): Future[Throwable \/ A] =
    fa transform {
      case Return(a) => value(a.right)
      case Throw(t) => value(t.left[A])
    }

  override def fail[A](e: Throwable): Future[A] =
    raiseError(e)
}
