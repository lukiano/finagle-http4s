package org.http4s.finagle

import java.util.concurrent.atomic.AtomicReference

import scalaz.concurrent.Task
import scalaz.std.option.none
import scalaz.stream.Process
import scalaz.syntax.monoid._
import scalaz.syntax.std.option._
import scalaz.Monoid

class ProcessStepper[A: Monoid](p: Process[Task, A]) {
  import scalaz.stream.Cause._
  import scalaz.stream.Process.{ Await, Emit, Halt, Step }

  private val cur = new AtomicReference[Process[Task, A]](p)

  def read: Task[Option[A]] = readFrom

  private val Done: Task[Option[A]] = Task.now(none[A])

  private def readFrom: Task[Option[A]] = {
    cur.get.step match {
      case s: Step[Task, A] @unchecked =>
        (s.head, s.next) match {
          case (Emit(os), cont) =>
            cur.set(cont.continue)
            Task.now(os.foldLeft[A](∅)((a, o) => a |+| o).some)
          case (awt: Await[Task, Any, A] @unchecked, cont) =>
            awt.evaluate flatMap { p =>
              cur.set(p +: cont)
              readFrom
            }
        }
      case Halt(End) =>
        Done
      case Halt(Kill) =>
        Done
      case Halt(Error(rsn)) =>
        Task.fail(rsn)
    }
  }
}
