package org.http4s
package finagle

import java.nio.channels.ClosedChannelException

import com.twitter.io.{ Buf, Reader, Writer }
import com.twitter.util.{ Future, FutureEventListener }

import org.log4s.getLogger

import scodec.bits.ByteVector
import scodec.interop.scalaz._

import scalaz.concurrent.Task
import scalaz.std.option._
import scalaz.stream.{ Sink, sink, Cause }
import scalaz.syntax.id._
import scalaz.syntax.monad._

class ProcessReader(st: EntityBody) extends Reader {
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
    (stepper.read map ReaderUtils.toBufLifted).asFuture().addEventListener(listener)

  def discard() =
    st.kill.run.run
}

object ReaderUtils {

  val toBuf: ByteVector => Buf = bv => Buf.ByteBuffer.Owned(bv.toByteBuffer)
  val toBufLifted: Option[ByteVector] => Option[Buf] = toBuf.lift

  val toBV: Buf => ByteVector = buf => ByteVector.view(Buf.ByteBuffer.Owned.extract(buf))
  val toBVLifted: Option[Buf] => Option[ByteVector] = toBV.lift

  case class WriteError(t: Throwable)

  def withProcessReader(st: EntityBody): Reader =
    new ProcessReader(st)

  def sinkWriter(writer: Writer): Sink[Task, ByteVector] =
    sink lift { bv => Task.suspend(writer.write(toBuf(bv)).asTask) }

  def withSinkWriter(st: EntityBody): Reader =
    Reader.writable() <| { writable =>
      st.to(sinkWriter(writable)).run.asFuture {
        case _ => st.kill.run.run
      }
    }

  def fromReader(reader: Reader): EntityBody =
    scalaz.stream.io.resource(
      Task.delay(reader)
    )(reader =>
        Task.delay {
          reader.discard()
        })(reader =>
        Task.suspend {
          reader.read(bufferSize).asTask flatMap (toBVLifted andThen {
            case None =>
              Task.fail(Cause.Terminated(Cause.End))
            case Some(buf) =>
              Task.now(buf)
          })
        })
}

