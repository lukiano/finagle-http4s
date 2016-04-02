package org.http4s.finagle

import java.util.concurrent.atomic.AtomicReference

import com.twitter.finagle.http._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Filter, Service, SimpleFilter, stats}
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Closable, Future}

import scalaz.syntax.id._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

private[finagle] object ServerDispatcher {

  def newServerDispatcher(
    trans:   Transport[Any, Any],
    service: Service[Request, Response],
    ss:      stats.StatsReceiver
  ): Closable = {
    val ref = new AtomicReference[Closable]
    val cl = Closable.ref(ref)
    val wrappedService = CloseIfErrorFilter(cl) :: Expect100ContinueFilter(trans) :: FixResponseHeaders(cl) :: service
    new codec.HttpServerDispatcher(trans, wrappedService, ss) <| ref.set
  }

  implicit class ServiceOps[ReqOut, RepIn](val s: Service[ReqOut, RepIn]) extends AnyVal {
    def ::[ReqIn, RepOut](f: Filter[ReqIn, RepOut, ReqOut, RepIn]): Service[ReqIn, RepOut] =
      f andThen s
  }

  case class CloseIfErrorFilter(closable: Closable) extends SimpleFilter[Request, Response] {
    def apply(request: Request, service: Service[Request, Response]): Future[Response] =
      service(request) onSuccess closeIfError(request)

    private def closeIfError(req: Request)(rep: Response): Unit =
      if (mayHaveRequestBody(req) && isError(rep)) {
        closable.close()
        ()
      }

    private def mayHaveRequestBody(req: Request) =
      req.method == Method.Put || req.method == Method.Patch

    private def isError(rep: Response) =
      rep.statusCode >= 400
  }

  case class Expect100ContinueFilter(trans: Transport[Any, Any]) extends SimpleFilter[Request, Response] {
    import java.util.concurrent.atomic.AtomicBoolean

    def apply(request: Request, service: Service[Request, Response]): Future[Response] =
      service(buildRequest(request))

    private def buildRequest(req: Request): Request =
      new RequestProxy {
        override val request = req
        override val reader: Reader = new Reader {
          val hasWritten100Response = new AtomicBoolean()
          def read(n: Int): Future[Option[Buf]] = {
            if (hasWritten100Response.compareAndSet(false, true)) {
              write100Response(req) >> req.reader.read(n)
            } else {
              req.reader.read(n)
            }
          }
          def discard() = req.reader.discard()
        }
      }

    private def write100Response(req: Request) =
      req.headerMap.get(Fields.Expect)
        .filter(_.trim == "100-continue")
        .cata(_ => trans.write(OneHundredContinueResponse), Future.Done)
  }

  case class FixResponseHeaders(closable: Closable) extends SimpleFilter[Request, Response] {
    def apply(request: Request, service: Service[Request, Response]): Future[Response] =
      service(request) map handleResponse(request)

    private def mayHaveChunkedResponseBody(req: Request, rep: Response) =
      !rep.contentLength.contains(0) && rep.isChunked && rep.statusCode < 300 && req.method == Method.Get

    private def isError(rep: Response) =
      rep.statusCode >= 400

    private def mayHaveRequestBody(req: Request) =
      req.method == Method.Put || req.method == Method.Patch

    private def handleResponse(req: Request)(rep: Response): Response = {
      if (mayHaveRequestBody(req) && isError(rep)) {
        closable.close()
      }
      mayHaveChunkedResponseBody(req, rep).fold(responseWithChunkedEncodingAndContentLength(rep), rep)
    }

    // A chunked-encoded response must not have a Content-Length header, but we need it.
    private def responseWithChunkedEncodingAndContentLength(rep: Response): Response =
      new ResponseProxy {
        import org.jboss.netty.handler.codec.http.DefaultHttpHeaders
        val response = rep
        override def reader = rep.reader
        override def headers() =
          (new DefaultHttpHeaders).add(super.headers()) // clone headers so they become "immutable"
      } <| { _.setChunked(true) }
  }
}