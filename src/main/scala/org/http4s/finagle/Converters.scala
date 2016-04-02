package org.http4s.finagle

import java.net.InetAddress

import com.twitter.finagle.http.{HeaderMap, MapHeaderMap, Method => FinagleMethod, Request => FinagleRequest, Response => FinagleResponse, ResponseProxy => FinagleResponseProxy, Status => FinagleStatus, Version => FinagleVersion}
import com.twitter.io.{Buf, Reader}
import com.twitter.util.Future

import org.http4s.{EntityBody, EmptyBody, Header => HHeader, Headers => HHeaders, HttpVersion => HVersion, Method => HMethod, Request => HRequest, Response => HResponse, Status => HStatus, Uri => HUri}

import scodec.bits.ByteVector
import scodec.interop.scalaz._

import scalaz.concurrent.Task
import scalaz.Isomorphism.<=>
import scalaz.std.indexedSeq._
import scalaz.stream.Cause.{End, Terminated}
import scalaz.stream.Process.{Emit, Step}
import scalaz.syntax.all._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.NonEmptyList

object Converters {

  val version: FinagleVersion <=> HVersion =
    new (FinagleVersion <=> HVersion) {
      val to: FinagleVersion => HVersion = {
        case FinagleVersion.Http10 => HVersion.`HTTP/1.0`
        case FinagleVersion.Http11 => HVersion.`HTTP/1.1`
      }
      val from: HVersion => FinagleVersion = {
        case HVersion.`HTTP/1.0` => FinagleVersion.Http10
        case HVersion.`HTTP/1.1` => FinagleVersion.Http11
      }
    }

  val headers: HeaderMap <=> HHeaders =
    new (HeaderMap <=> HHeaders) {
      private def nel[A](list: List[A]): NonEmptyList[A] = NonEmptyList.nel(list.head, list.tail)

      val to: HeaderMap => HHeaders = h =>
        HHeaders {
          h.keySet.toList map { name => HHeader(name, h.getAll(name).mkString(",")).parsed }
        }
      val from: HHeaders => HeaderMap = h => {
        val headerMap = MapHeaderMap()
        h.foreach { header =>
          header.value.split(',').foreach { value =>
            headerMap += header.name.value -> value
            ()
          }
        }
        headerMap
      }
    }

  val method: FinagleMethod <=> HMethod =
    new (FinagleMethod <=> HMethod) {
      val to: FinagleMethod => HMethod = {
        case FinagleMethod.Get     => HMethod.GET
        case FinagleMethod.Post    => HMethod.POST
        case FinagleMethod.Put     => HMethod.PUT
        case FinagleMethod.Head    => HMethod.HEAD
        case FinagleMethod.Patch   => HMethod.PATCH
        case FinagleMethod.Delete  => HMethod.DELETE
        case FinagleMethod.Trace   => HMethod.TRACE
        case FinagleMethod.Connect => HMethod.CONNECT
        case FinagleMethod.Options => HMethod.OPTIONS
        case other                 => HMethod.fromString(other.toString()).fold(_ => throw new IllegalArgumentException, identity)
      }
      val from: HMethod => FinagleMethod = {
        case HMethod.GET                => FinagleMethod.Get
        case HMethod.POST               => FinagleMethod.Post
        case HMethod.PUT                => FinagleMethod.Put
        case HMethod.HEAD               => FinagleMethod.Head
        case HMethod.PATCH              => FinagleMethod.Patch
        case HMethod.DELETE             => FinagleMethod.Delete
        case HMethod.TRACE              => FinagleMethod.Trace
        case HMethod.CONNECT            => FinagleMethod.Connect
        case HMethod.OPTIONS            => FinagleMethod.Options
        case other                     => FinagleMethod(other.name)
      }
    }

  val status: FinagleStatus <=> HStatus =
    new (FinagleStatus <=> HStatus) {
      val to: FinagleStatus => HStatus = fs =>
        HStatus.fromInt(fs.code).fold(_ => HStatus.InternalServerError, identity)
      val from: HStatus => FinagleStatus = s =>
        FinagleStatus(s.code)
    }

  def buildRequest(req: FinagleRequest): HRequest = {
    val toBV: Option[Buf] => Task[ByteVector] = {
      case None =>
        Task.fail(Terminated(End))
      case Some(buf) =>
        Task.now(ByteVector.view(Buf.ByteBuffer.Owned.extract(buf)))
    }
    val stream = hasBody(req).option {
      scalaz.stream.io.resource(
        Task.delay(req.reader)
      )(reader =>
        Task.delay {
          reader.discard()
        })(reader =>
        reader.read(bufferSize).asTask >>= toBV)

    }
    HRequest(
      method = method.to(req.method),
      uri = HUri.fromString(req.uri).fold(_ => throw new IllegalArgumentException, identity),
      httpVersion = version.to(req.version),
      headers = headers.to(req.headerMap),
      body = stream getOrElse EmptyBody
    )
  }

  private def hasBody(request: FinagleRequest): Boolean =
    request.isChunked || request.contentLength.exists(_ > 0)


  def handleResponse(rep: HResponse): FinagleResponse = {
    new FinagleResponseProxy {
      private def getBody(stream: EntityBody): Option[ByteVector] = // retrieve content from stream if it's an Emit
        stream.step match {
          case s: Step[Task, ByteVector] =>
            s.head match {
              case Emit(seqbv) => Some(seqbv.toIndexedSeq.sumr)
              case _ => None
            }
          case _ => None
        }
      private val responseStream = rep.body.isHalt.option(rep.body) filter (_ => rep.contentLength.contains(0)) // ignore stream if 0-length
      private val body = responseStream flatMap getBody
      override val response = FinagleResponse(Converters.version.from(rep.httpVersion), Converters.status.from(rep.status))
      body.foreach { bv => content = Buf.ByteBuffer.Owned.apply(bv.toByteBuffer) }
      override val isChunked = responseStream.isDefined && body.isEmpty
      override val reader =
        responseStream.cata(st => new ProcessReader(st), EmptyReader)
    }
  }

  private object EmptyReader extends Reader {
    def read(n: Int): Future[Option[Buf]] = Future.value(None)
    def discard(): Unit = ()
  }
}
