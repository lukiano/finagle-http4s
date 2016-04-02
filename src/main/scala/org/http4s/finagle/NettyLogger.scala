package org.http4s.finagle

import org.jboss.netty.logging.InternalLoggerFactory
import org.log4s.getLogger

object NettyLogger {
  def apply(): Unit =
    InternalLoggerFactory.setDefaultFactory(new InternalLoggerFactory {
      def newInstance(s: String) = new Log4sNettyLogger(getLogger(s))
    })
}