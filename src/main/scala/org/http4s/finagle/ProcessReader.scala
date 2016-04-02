package org.http4s.finagle

import java.nio.channels.ClosedChannelException

import com.twitter.io._
import com.twitter.util._
import scodec.bits.ByteVector

import org.log4s.getLogger
import scalaz.std.option._
import scalaz.stream.Process
import scalaz.syntax.monad._
import scalaz.concurrent.Task
import scodec.interop.scalaz._

class ProcessReader(st: Process[Task, ByteVector]) extends Reader {
  import ProcessReader._

  private val logger = getLogger

  private val listener = new FutureEventListener[Option[Buf]] {
    def onFailure(cause: Throwable): Unit = cause match {
      case _: ClosedChannelException => logger.info(cause)("Exception while processing stream")
      case _                         => logger.warn(cause)("Exception while processing stream")
    }
    def onSuccess(value: Option[Buf]): Unit = ()
  }

  private val stepper = new ProcessStepper(st filter { _.length > 0 })

  def read(ignored: Int): Future[Option[Buf]] =
    (stepper.read map toBufLifted).asFuture().addEventListener(listener)

  def discard(): Unit = Await.result(st.kill.run.asFuture())
}

object ProcessReader {
  private[ProcessReader] val toBuf: ByteVector => Buf = bv => Buf.ByteBuffer.Owned(bv.toByteBuffer)
  private[ProcessReader] val toBufLifted: Option[ByteVector] => Option[Buf] = toBuf.lift
}
