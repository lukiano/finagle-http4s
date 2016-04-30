package org.http4s
package finagle

import java.util.concurrent.atomic.AtomicReference

import com.twitter.io.{ Buf, Reader }
import com.twitter.finagle.{ Filter, Service, SimpleFilter }
import com.twitter.finagle.http.codec.HttpServerDispatcher
import com.twitter.finagle.http.{ Fields, Method => FinagleMethod, Request => FinagleRequest, RequestProxy => FinagleRequestProxy, Response => FinagleResponse, ResponseProxy => FinagleResponseProxy }
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{ Closable, Future }

import scalaz.syntax.all._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import FinagleConverters._

private[finagle] object ServerDispatcher {

  def newServerDispatcher(
    trans:   Transport[Any, Any],
    service: Service[Request, Response],
    stats:   StatsReceiver
  ): Closable = {
    val ref = new AtomicReference[Closable]
    val cl = Closable.ref(ref) //aggregateService.dimap(request.to, response.from).mapK(_.asFuture()).run
    val wrappedService = CloseIfErrorFilter(cl) :: Expect100ContinueFilter(trans) :: FixResponseHeaders(cl) :: AdapterFilter :: service
    new HttpServerDispatcher(trans, wrappedService, stats) <| ref.set
  }

  implicit class ServiceOps[ReqOut, RepIn](val s: Service[ReqOut, RepIn]) extends AnyVal {
    def ::[ReqIn, RepOut](f: Filter[ReqIn, RepOut, ReqOut, RepIn]): Service[ReqIn, RepOut] =
      f andThen s
  }

  case object AdapterFilter extends Filter[FinagleRequest, FinagleResponse, Request, Response] {
    def apply(req: FinagleRequest, service: Service[Request, Response]): Future[FinagleResponse] =
      service(request.to(req)) map response.from
  }

  case class CloseIfErrorFilter(closable: Closable) extends SimpleFilter[FinagleRequest, FinagleResponse] {
    def apply(request: FinagleRequest, service: Service[FinagleRequest, FinagleResponse]): Future[FinagleResponse] =
      service(request) onSuccess closeIfError(request)

    private def closeIfError(req: FinagleRequest)(rep: FinagleResponse): Unit =
      if (mayHaveRequestBody(req) && isError(rep)) {
        closable.close()
        ()
      }

    private def mayHaveRequestBody(req: FinagleRequest): Boolean =
      req.method == FinagleMethod.Put || req.method == FinagleMethod.Patch

    private def isError(rep: FinagleResponse): Boolean =
      rep.statusCode >= 400
  }

  case class Expect100ContinueFilter(trans: Transport[Any, Any]) extends SimpleFilter[FinagleRequest, FinagleResponse] {
    import java.util.concurrent.atomic.AtomicBoolean

    def apply(request: FinagleRequest, service: Service[FinagleRequest, FinagleResponse]): Future[FinagleResponse] =
      service(buildRequest(request))

    private def buildRequest(req: FinagleRequest): FinagleRequest =
      new FinagleRequestProxy {
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

    private def write100Response(req: FinagleRequest): Future[Unit] =
      req.headerMap.get(Fields.Expect)
        .filter(_.trim == "100-continue")
        .cata(_ => trans.write(OneHundredContinueResponse), Future.Done)
  }

  case class FixResponseHeaders(closable: Closable) extends SimpleFilter[FinagleRequest, FinagleResponse] {
    def apply(request: FinagleRequest, service: Service[FinagleRequest, FinagleResponse]): Future[FinagleResponse] =
      service(request) map handleResponse(request)

    private def mayHaveChunkedResponseBody(req: FinagleRequest, rep: FinagleResponse) =
      !rep.contentLength.contains(0) && rep.isChunked && rep.statusCode < 300 && req.method == FinagleMethod.Get

    private def isError(rep: FinagleResponse): Boolean =
      rep.statusCode >= 400

    private def mayHaveRequestBody(req: FinagleRequest): Boolean =
      req.method == FinagleMethod.Put || req.method == FinagleMethod.Patch

    private def handleResponse(req: FinagleRequest)(rep: FinagleResponse): FinagleResponse = {
      if (mayHaveRequestBody(req) && isError(rep)) {
        closable.close()
      }
      mayHaveChunkedResponseBody(req, rep).fold(responseWithChunkedEncodingAndContentLength(rep), rep)
    }

    // A chunked-encoded response must not have a Content-Length header, but we need it.
    private def responseWithChunkedEncodingAndContentLength(rep: FinagleResponse): FinagleResponse =
      new FinagleResponseProxy {
        import org.jboss.netty.handler.codec.http.DefaultHttpHeaders
        val response = rep
        override def reader = rep.reader
        override def headers() =
          (new DefaultHttpHeaders).add(super.headers()) // clone headers so they become "immutable"
      } <| { _.setChunked(true) }
  }
}