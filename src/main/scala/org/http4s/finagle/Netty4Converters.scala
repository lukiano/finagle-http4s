package org.http4s
package finagle

import java.util.Arrays.asList

import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{ DefaultFullHttpResponse, DefaultHttpHeaders, DefaultHttpResponse, HttpHeaders => NettyHeaders, HttpMethod => NettyMethod, HttpRequest => NettyRequest, HttpResponse => NettyResponse, HttpResponseStatus => NettyStatus, HttpVersion => NettyVersion }
import scodec.bits.ByteVector
import scodec.interop.scalaz._

import scala.collection.convert.decorateAsScala._
import scalaz.Isomorphism.<=>
import scalaz.NonEmptyList
import scalaz.concurrent.Task
import scalaz.std.indexedSeq._
import scalaz.stream.Process.{ Emit, Step }
import scalaz.syntax.all._
import scalaz.syntax.std.option._

object Netty4Converters {

  val version: NettyVersion <=> HttpVersion =
    new (NettyVersion <=> HttpVersion) {
      val to: NettyVersion => HttpVersion = {
        case NettyVersion.HTTP_1_0 => HttpVersion.`HTTP/1.0`
        case NettyVersion.HTTP_1_1 => HttpVersion.`HTTP/1.1`
      }
      val from: HttpVersion => NettyVersion = {
        case HttpVersion.`HTTP/1.0` => NettyVersion.HTTP_1_0
        case HttpVersion.`HTTP/1.1` => NettyVersion.HTTP_1_1
      }
    }

  val headers: NettyHeaders <=> Headers =
    new (NettyHeaders <=> Headers) {
      private def nel[A](list: List[A]): NonEmptyList[A] = NonEmptyList.nel(list.head, list.tail)

      val to: NettyHeaders => Headers = h =>
        Headers {
          h.names().asScala.toList map { name => Header(name, h.getAll(name).asScala.mkString(",")).parsed }
        }
      val from: Headers => NettyHeaders = h => {
        val nettyHeaders = new DefaultHttpHeaders()
        h.foreach { header =>
          val values = asList(header.value.split(',').map(_.trim): _*)
          nettyHeaders.add(header.name.value, values)
        }
        nettyHeaders
      }
    }

  val method: NettyMethod <=> Method =
    new (NettyMethod <=> Method) {
      val to: NettyMethod => Method = {
        case NettyMethod.GET     => Method.GET
        case NettyMethod.POST    => Method.POST
        case NettyMethod.PUT     => Method.PUT
        case NettyMethod.HEAD    => Method.HEAD
        case NettyMethod.PATCH   => Method.PATCH
        case NettyMethod.DELETE  => Method.DELETE
        case NettyMethod.TRACE   => Method.TRACE
        case NettyMethod.CONNECT => Method.CONNECT
        case NettyMethod.OPTIONS => Method.OPTIONS
        case other               => Method.fromString(other.name).fold(_ => throw new IllegalArgumentException, identity)
      }
      val from: Method => NettyMethod = {
        case Method.GET     => NettyMethod.GET
        case Method.POST    => NettyMethod.POST
        case Method.PUT     => NettyMethod.PUT
        case Method.HEAD    => NettyMethod.HEAD
        case Method.PATCH   => NettyMethod.PATCH
        case Method.DELETE  => NettyMethod.DELETE
        case Method.TRACE   => NettyMethod.TRACE
        case Method.CONNECT => NettyMethod.CONNECT
        case Method.OPTIONS => NettyMethod.OPTIONS
        case other          => NettyMethod.valueOf(other.name)
      }
    }

  val status: NettyStatus <=> Status =
    new (NettyStatus <=> Status) {
      val to: NettyStatus => Status = ns =>
        Status.fromInt(ns.code).fold(_ => Status.InternalServerError, identity)
      val from: Status => NettyStatus = s =>
        NettyStatus.valueOf(s.code)
    }

  def buildRequest(req: NettyRequest): Request =
    Request(
      method = method.to(req.method),
      uri = Uri.fromString(req.uri).fold(t => throw t, identity),
      httpVersion = version.to(req.protocolVersion),
      headers = headers.to(req.headers()),
      body = EmptyBody
    )

  def handleResponse(rep: Response): NettyResponse = {
    val responseStream = rep.body.some filter (!_.isHalt) filter (_ => !rep.contentLength.contains(0))

    def getBody(stream: EntityBody): Option[ByteVector] = // retrieve content from stream if it's an Emit
      stream.step match {
        case s: Step[Task, ByteVector] =>
          s.head match {
            case Emit(seqbv) => Some(seqbv.toIndexedSeq.sumr)
            case _           => None
          }
        case _ => None
      }

    val nettyResponse: NettyResponse = (responseStream flatMap getBody).cata(
      bv => new DefaultFullHttpResponse(version.from(rep.httpVersion), status.from(rep.status), Unpooled.wrappedBuffer(bv.toByteBuffer)),
      new DefaultHttpResponse(version.from(rep.httpVersion), status.from(rep.status))
    )
    nettyResponse.headers().add(headers.from(rep.headers))
    nettyResponse
  }

  private[finagle] object OneHundredContinueResponse extends DefaultHttpResponse(NettyVersion.HTTP_1_1, NettyStatus.CONTINUE)
}
