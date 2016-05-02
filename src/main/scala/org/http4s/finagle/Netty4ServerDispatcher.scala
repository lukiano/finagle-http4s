package org.http4s
package finagle

import java.net.InetSocketAddress

import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ Service => FinagleService }
import com.twitter.util.{ Future, Promise, Return, Throw }
import io.netty.buffer._
import io.netty.handler.codec.http._
import Netty4Converters._
import org.log4s.getLogger
import scodec.bits.ByteVector
import scalaz.{ -\/, \/, \/- }
import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream.{ Cause, Process, Sink }
import scalaz.syntax.id._

/**
 * Bypasses Finagle's HttpDispatcher and Http API and talks directly to the transport.
 * Dependent of the Transport messages implementation: Netty 3
 */

private[finagle] class Netty4ServerDispatcher(
    trans:   Transport[Any, Any],
    service: FinagleService[Request, Response]
) extends GenSerialServerDispatcher[Request, Response, Any, Any](trans) {

  private[this] val logger = getLogger

  private[finagle] def sink(transport: Transport[Any, Any]): Sink[Task, ByteVector] =
    scalaz.stream.sink.lift { data =>
      transport.write {
        new DefaultHttpContent(Unpooled.wrappedBuffer(data.toByteBuffer))
      }.asTask
    }

  private[this] def writeChunks(rep: Response): Future[Unit] =
    rep.body.to[Task] {
      sink(trans)
    }.run.asFuture {
      case e => () <| { _ =>
        rep.body.kill.run.attemptRun
      }
    } transform {
      case Return(_) =>
        trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
      case Throw(t) =>
        logger.warn(t)("While trying to write to Transport")
        close()
        trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
    }

  private def readChunks: Process[Task, ByteVector] = {
    def consume: Task[ByteVector \/ ByteVector] = // left means is the last bytevector
      trans.read().asTask flatMap {
        case last: LastHttpContent =>
          val content = last.content()
          if (content.isReadable) {
            Task.now(toBv(content).left)
          } else {
            Task.fail(Cause.Terminated(Cause.End))
          }
        case chunk: HttpContent =>
          val content = chunk.content() // unwrap(chunk.content())
          if (content.isReadable()) {
            Task.now(toBv(content).right)
          } else {
            consume
          }
        case unexpected =>
          Task.fail(new IllegalStateException(s"Unexpected message $unexpected"))
      }

    def step: Process[Task, ByteVector] =
      await(Task.suspend(consume)) {
        case \/-(bv) => emit(bv) ++ step // right
        case -\/(bv) => emit(bv) ++ halt
      }
    step
  }

  private def toBv(content: ByteBuf): ByteVector =
    if (content.hasArray && content.arrayOffset() == 0) {
      ByteVector.view(content.array)
    } else {
      ByteVector.view(content.nioBuffer)
    }

  protected override def dispatch(req: Any, eos: Promise[Unit]): Future[Response] = req match {
    case req: FullHttpRequest =>
      val stream: EntityBody = {
        val content = req.content
        if (content.isReadable) {
          Process.emit(toBv(content))
        } else Process.halt
      } onHalt { _.asHalt }

      val connection = Request.Connection(
        trans.localAddress.asInstanceOf[InetSocketAddress],
        trans.remoteAddress.asInstanceOf[InetSocketAddress],
        trans.peerCertificate.isDefined
      )
      service(buildRequest(req).copy(
        body = stream,
        attributes = AttributeMap(
          AttributeEntry(Request.Keys.ConnectionInfo, connection),
          AttributeEntry(Request.Keys.ServerSoftware, server.ServerSoftware("finagle-netty4"))
        )
      )).ensure {
        eos.setDone()
        ()
      }

    case req: HttpRequest =>
      val stream: EntityBody = {
        val t: Task[Unit] =
          if (HttpUtil.is100ContinueExpected(req))
            Task.suspend {
              trans.write(OneHundredContinueResponse).asTask
            }
          else
            Task.now(())
        Process.eval(t) flatMap { _ => readChunks }
      } onHalt { _.asHalt }

      val connection = Request.Connection(
        trans.localAddress.asInstanceOf[InetSocketAddress],
        trans.remoteAddress.asInstanceOf[InetSocketAddress],
        trans.peerCertificate.isDefined
      )
      service(buildRequest(req).copy(
        body = stream,
        attributes = AttributeMap(
          AttributeEntry(Request.Keys.ConnectionInfo, connection),
          AttributeEntry(Request.Keys.ServerSoftware, server.ServerSoftware("finagle-netty4"))
        )
      )).ensure {
        eos.setDone()
        ()
      }

    case invalid =>
      eos.setDone()
      Future.exception(new IllegalArgumentException("Invalid message " + invalid))
  }

  protected def handle(rep: Response): Future[Unit] = {
    import HttpHeaderNames._
    import HttpHeaderValues._
    val nettyResponse = handleResponse(rep)

    if (nettyResponse.status.code >= 400) {
      nettyResponse.headers().set(CONNECTION, CLOSE)
    }

    if (rep.body.isHalt) {
      rep.body.run asFuture {
        case e => () <| { _ =>
          rep.body.kill.run.attemptRun
        }
      } transform {
        case Return(_) =>
          trans.write(nettyResponse)
        case Throw(t) =>
          close()
          logger.warn(t)("While trying to write to Transport")
          nettyResponse.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
          nettyResponse.headers().set(CONTENT_LENGTH, 0)
          nettyResponse.headers().set(CONNECTION, CLOSE)
          trans.write(nettyResponse)
      }
    } else if (nettyResponse.isInstanceOf[FullHttpResponse]) {
      trans.write(nettyResponse)
    } else {
      if (rep.contentLength.isEmpty) {
        nettyResponse.headers().set(TRANSFER_ENCODING, CHUNKED)
      }
      trans.write(nettyResponse) before writeChunks(rep)
    }
  }
}