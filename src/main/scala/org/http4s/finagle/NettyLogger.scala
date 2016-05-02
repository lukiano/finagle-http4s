package org.http4s.finagle

object NettyLogger {
  def netty3(): Unit =
    org.jboss.netty.logging.InternalLoggerFactory.setDefaultFactory(new org.jboss.netty.logging.Slf4JLoggerFactory())

  def netty4(): Unit =
    io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(io.netty.util.internal.logging.Slf4JLoggerFactory.INSTANCE)
}