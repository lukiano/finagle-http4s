package org.http4s

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.util._
import com.twitter.finagle.tracing.Trace
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpResponseStatus, HttpVersion => Version}

import scalaz.concurrent.Task
import scalaz.syntax.id._

package object finagle {
  val bufferSize = 32768

  implicit object TwitterFutureMonad extends FutureMonad

  implicit class TwitterFutureToTask[A](val f: Future[A]) extends AnyVal {
    def asTask: Task[A] = Task.async { cb =>
      f.respond {
        case Return(a) => cb(a.right)
        case Throw(t)  => cb(t.left)
      }
    }
  }

  implicit class ScalazTaskToTwitterFuture[A](val t: Task[A]) extends AnyVal {
    def asFuture(handler: PartialFunction[Throwable, Unit] = PartialFunction.empty): Future[A] =
      new Promise[A] <| { p =>
        val cancel = new AtomicBoolean()
        p.setInterruptHandler {
          case th =>
            cancel.set(true)
            if (handler.isDefinedAt(th)) handler(th)
        }
        t.runAsyncInterruptibly({ _.fold(p.setException, p.setValue) }, cancel)
      }
  }

  private[finagle] object OneHundredContinueResponse
    extends DefaultHttpResponse(Version.HTTP_1_1, HttpResponseStatus.CONTINUE)

  def disableTracer(): Unit =
    Trace.disable()
}
