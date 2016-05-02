package com.twitter.finagle.netty4

import java.lang.{ Boolean => JBool, Integer => JInt }
import java.net.SocketAddress
import java.util.concurrent.TimeUnit

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle._
import com.twitter.finagle.netty4.channel.{ Netty4ServerChannelInitializer, ServerBridge }
import com.twitter.finagle.server.Listener
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.{ GenericFutureListener, Future => NettyFuture }

/**
 * Netty4 TLS configuration.
 *
 * @param newEngine Creates a new SSL engine
 */
private[finagle] case class Netty4ListenerTLSConfig(newEngine: () => Engine)

private[finagle] object Netty4Listener {
  val TrafficClass: ChannelOption[JInt] = ChannelOption.newInstance("trafficClass")
}

private[finagle] case class PipelineInit(cf: ChannelPipeline => Unit) {
  def mk(): (PipelineInit, Stack.Param[PipelineInit]) =
    (this, PipelineInit.param)
}
private[finagle] object PipelineInit {
  implicit val param = Stack.Param(PipelineInit(_ => ()))
}

case class Native(epoll: Boolean) {
  def mk(): (Native, Stack.Param[Native]) =
    (this, Native.param)
}
object Native {
  implicit val param = Stack.Param(Native(false))
}

/**
 * Constructs a `Listener[In, Out]` given a ``pipelineInit`` function
 * responsible for framing a [[Transport]] stream. The [[Listener]] is configured
 * via the passed in [[com.twitter.finagle.Stack.Param Params]].
 *
 * @see [[com.twitter.finagle.server.Listener]]
 * @see [[com.twitter.finagle.transport.Transport]]
 * @see [[com.twitter.finagle.param]]
 */
private[finagle] case class Netty4Listener[In, Out](
    params:           Stack.Params,
    transportFactory: SocketChannel => Transport[In, Out] = ChannelTransport[In, Out](_)
) extends Listener[In, Out] {

  private[this] val PipelineInit(pipelineInit) = params[PipelineInit]

  // transport params
  private[this] val Transport.Liveness(_, _, keepAlive) = params[Transport.Liveness]
  private[this] val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
  private[this] val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]
  private[this] val Native(epoll) = params[Native]

  // listener params
  private[this] val Listener.Backlog(backlog) = params[Listener.Backlog]

  // netty4 params
  private[this] val param.Allocator(allocator) = params[param.Allocator]

  /**
   * Listen for connections and apply the `serveTransport` callback on connected [[Transport transports]].
   *
   * @param addr socket address for listening.
   * @param serveTransport a call-back for newly created transports which in turn are
   *                       created for new connections.
   * @note the ``serveTransport`` implementation is responsible for calling
   *       [[Transport.close() close]] on  [[Transport transports]].
   */
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer =
    new ListeningServer with CloseAwaitably {

      val newBridge = () => new ServerBridge(
        transportFactory,
        serveTransport
      )
      val threadFactory = new NamedPoolThreadFactory("finagle/netty4/boss", makeDaemons = true)
      val bossLoop: EventLoopGroup =
        if (epoll) {
          this.getClass.getClassLoader
            .loadClass("io.netty.channel.epoll.EpollEventLoopGroup")
            .getConstructor(classOf[java.lang.Integer], classOf[java.util.concurrent.ThreadFactory])
            .newInstance(java.lang.Integer.valueOf(1) /*nThreads*/ , threadFactory)
            .asInstanceOf[EventLoopGroup]
        } else {
          new NioEventLoopGroup(1 /*nThreads*/ , threadFactory)
        }

      val bootstrap = new ServerBootstrap()
      if (epoll)
        bootstrap.channel(this.getClass.getClassLoader
          .loadClass("io.netty.channel.epoll.EpollServerSocketChannel")
          .asInstanceOf[Class[ServerChannel]])
      else
        bootstrap.channel(classOf[NioServerSocketChannel])
      bootstrap.group(bossLoop, WorkerPool)
      bootstrap.childOption[JBool](ChannelOption.TCP_NODELAY, noDelay)

      bootstrap.option(ChannelOption.ALLOCATOR, allocator)
      bootstrap.childOption(ChannelOption.ALLOCATOR, allocator)
      bootstrap.option[JBool](ChannelOption.SO_REUSEADDR, reuseAddr)
      bootstrap.option[JInt](ChannelOption.SO_LINGER, 0)
      backlog.foreach(bootstrap.option[JInt](ChannelOption.SO_BACKLOG, _))
      sendBufSize.foreach(bootstrap.childOption[JInt](ChannelOption.SO_SNDBUF, _))
      recvBufSize.foreach(bootstrap.childOption[JInt](ChannelOption.SO_RCVBUF, _))
      keepAlive.foreach(bootstrap.childOption[JBool](ChannelOption.SO_KEEPALIVE, _))
      params[Listener.TrafficClass].value.foreach { tc =>
        bootstrap.option[JInt](Netty4Listener.TrafficClass, tc)
        bootstrap.childOption[JInt](Netty4Listener.TrafficClass, tc)
      }

      val initializer = new Netty4ServerChannelInitializer(pipelineInit, params, newBridge)
      bootstrap.childHandler(initializer)

      // Block until listening socket is bound. `ListeningServer`
      // represents a bound server and if we don't block here there's
      // a race between #listen and #boundAddress being available.
      val ch = bootstrap.bind(addr).awaitUninterruptibly().channel()

      /**
       * Immediately close the listening socket then shutdown the netty
       * boss threadpool with ``deadline`` timeout for existing tasks.
       *
       * @return a [[Future]] representing the shutdown of the boss threadpool.
       */
      def closeServer(deadline: Time) = closeAwaitably {
        // note: this ultimately calls close(2) on
        // a non-blocking socket so it should not block.
        ch.close().awaitUninterruptibly()

        val p = new Promise[Unit]

        // The boss loop immediately starts refusing new work.
        // Existing tasks have ``deadline`` time to finish executing.
        val shutdown: NettyFuture[_] = bossLoop
          .shutdownGracefully(0 /* quietPeriod */ , deadline.inMillis /* timeout */ , TimeUnit.MILLISECONDS)
        def shutdownListener[N <: NettyFuture[_]]: GenericFutureListener[N] =
          new GenericFutureListener[N] {
            def operationComplete(future: N): Unit = {
              p.setDone()
              ()
            }
          }
        shutdown.addListener(shutdownListener)
        p
      }

      def boundAddress: SocketAddress = ch.localAddress()
    }
}

case class ChannelTransport[In, Out](ch: Channel) extends transport.ChannelTransport[In, Out](ch)
