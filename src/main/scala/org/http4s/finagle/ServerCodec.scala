package org.http4s.finagle

import com.twitter.finagle.http.{ Request, Response }
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ Codec, CodecFactory, Service }
import com.twitter.util.Closable
import org.jboss.netty.channel.{ ChannelPipelineFactory, Channels }
import org.jboss.netty.handler.codec.http.HttpServerCodec

import scalaz.syntax.id._

class ServerCodec(stats: StatsReceiver) extends CodecFactory[Request, Response] {
  def server = Function.const {
    new Codec[Request, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline =
          Channels.pipeline() <| { _.addLast("httpCodec", new HttpServerCodec) }
      }
      override def newServerDispatcher(
        transport: Transport[Any, Any],
        service: Service[Request, Response]
      ): Closable =
        ServerDispatcher.newServerDispatcher(transport, service, stats)
    }
  }

  def client = throw new RuntimeException()
}