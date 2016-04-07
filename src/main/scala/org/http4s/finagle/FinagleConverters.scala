package org.http4s
package finagle

import com.twitter.finagle.http.{ HeaderMap, MapHeaderMap, Method => FinagleMethod, Request => FinagleRequest, Response => FinagleResponse, ResponseProxy => FinagleResponseProxy, Status => FinagleStatus, Version => FinagleVersion }
import com.twitter.io.{ Buf, Reader }
import com.twitter.util.Future
import scodec.bits.ByteVector
import scodec.interop.scalaz._

import scalaz.Isomorphism.<=>
import scalaz.concurrent.Task
import scalaz.std.indexedSeq._
import scalaz.stream.Process.{ Emit, Step }
import scalaz.syntax.all._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

object FinagleConverters {

  val version: FinagleVersion <=> HttpVersion =
    new (FinagleVersion <=> HttpVersion) {
      val to: FinagleVersion => HttpVersion = {
        case FinagleVersion.Http10 => HttpVersion.`HTTP/1.0`
        case FinagleVersion.Http11 => HttpVersion.`HTTP/1.1`
      }
      val from: HttpVersion => FinagleVersion = {
        case HttpVersion.`HTTP/1.0` => FinagleVersion.Http10
        case HttpVersion.`HTTP/1.1` => FinagleVersion.Http11
      }
    }

  val headers: HeaderMap <=> Headers =
    new (HeaderMap <=> Headers) {
      val to: HeaderMap => Headers = h =>
        Headers {
          h.keySet.toList map { name => Header(name, h.getAll(name).mkString(",")).parsed }
        }
      val from: Headers => HeaderMap = h => {
        val map = scala.collection.mutable.Map.empty[String, Seq[String]]
        h.foreach { header =>
          map += (header.name.value -> header.value.split(',').map(_.trim))
        }
        new MapHeaderMap(map)
      }
    }

  val method: FinagleMethod <=> Method =
    new (FinagleMethod <=> Method) {
      val to: FinagleMethod => Method = {
        case FinagleMethod.Get => Method.GET
        case FinagleMethod.Post => Method.POST
        case FinagleMethod.Put => Method.PUT
        case FinagleMethod.Head => Method.HEAD
        case FinagleMethod.Patch => Method.PATCH
        case FinagleMethod.Delete => Method.DELETE
        case FinagleMethod.Trace => Method.TRACE
        case FinagleMethod.Connect => Method.CONNECT
        case FinagleMethod.Options => Method.OPTIONS
        case other => Method.fromString(other.toString()).fold(_ => throw new IllegalArgumentException, identity)
      }
      val from: Method => FinagleMethod = {
        case Method.GET => FinagleMethod.Get
        case Method.POST => FinagleMethod.Post
        case Method.PUT => FinagleMethod.Put
        case Method.HEAD => FinagleMethod.Head
        case Method.PATCH => FinagleMethod.Patch
        case Method.DELETE => FinagleMethod.Delete
        case Method.TRACE => FinagleMethod.Trace
        case Method.CONNECT => FinagleMethod.Connect
        case Method.OPTIONS => FinagleMethod.Options
        case other => FinagleMethod(other.name)
      }
    }

  val status: FinagleStatus <=> Status =
    new (FinagleStatus <=> Status) {
      val to: FinagleStatus => Status = fs =>
        Status.fromInt(fs.code).fold(_ => Status.InternalServerError, identity)
      val from: Status => FinagleStatus = s =>
        FinagleStatus(s.code)
    }

  val request: FinagleRequest <=> Request =
    new (FinagleRequest <=> Request) {
      val to: FinagleRequest => Request = req =>
        Request(
          method = method.to(req.method),
          uri = Uri.fromString(req.uri).fold(_ => throw new IllegalArgumentException, identity),
          httpVersion = version.to(req.version),
          headers = headers.to(req.headerMap),
          body = hasBody(req).fold(ReaderUtils.fromReader(req.reader), EmptyBody)
        )
      val from: Request => FinagleRequest = req =>
        FinagleRequest(
          version = version.from(req.httpVersion),
          method = method.from(req.method),
          uri = req.uri.renderString,
          reader = ReaderUtils.withProcessReader(req.body)
        )
    }

  private def hasBody(request: FinagleRequest): Boolean =
    request.isChunked || request.contentLength.exists(_ > 0)

  val response: FinagleResponse <=> Response =
    new (FinagleResponse <=> Response) {
      val to: FinagleResponse => Response = rep =>
        Response(
          status = status.to(rep.status),
          httpVersion = version.to(rep.version),
          headers = headers.to(rep.headerMap),
          body = EmptyBody // rep.content.isEmpty
        )
      val from: Response => FinagleResponse = rep =>
        new FinagleResponseProxy {
          private val responseStream = rep.body.some filter (!_.isHalt) filter (_ => !rep.contentLength.contains(0)) // ignore stream if 0-length
          private val body = responseStream flatMap getBody

          override val response = FinagleResponse(FinagleConverters.version.from(rep.httpVersion), FinagleConverters.status.from(rep.status))

          response.headerMap setAll FinagleConverters.headers.from(rep.headers)

          private def getBody(stream: EntityBody): Option[ByteVector] = // retrieve content from stream if it's an Emit
            stream.step match {
              case s: Step[Task, ByteVector] =>
                s.head match {
                  case Emit(seqbv) => Some(seqbv.toIndexedSeq.sumr)
                  case _ => None
                }
              case _ => None
            }
          body.foreach { bv => content = ReaderUtils.toBuf(bv) }

          override val isChunked = responseStream.isDefined && body.isEmpty

          override val reader =
            responseStream.cata(st => ReaderUtils.withProcessReader(st), EmptyReader)
        }
    }

  private object EmptyReader extends Reader {
    def read(n: Int): Future[Option[Buf]] = Future.value(None)
    def discard(): Unit = ()
  }

  implicit class HeaderMapOps(val hm: HeaderMap) extends AnyVal {
    def setAll(other: HeaderMap): Unit =
      for (key <- other.keys) {
        hm.set(key, other.getAll(key).mkString(", "))
      }
  }
}
