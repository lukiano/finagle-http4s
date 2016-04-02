package org.http4s.finagle

import org.jboss.netty.logging.AbstractInternalLogger
import org.log4s.Logger

class Log4sNettyLogger(logger: Logger) extends AbstractInternalLogger {
  override def warn(msg: String): Unit = logger.warn(msg)

  override def warn(msg: String, cause: Throwable): Unit = logger.warn(cause)(msg)

  override def isErrorEnabled: Boolean = logger.isErrorEnabled

  override def isInfoEnabled: Boolean = logger.isInfoEnabled

  override def isDebugEnabled: Boolean = logger.isDebugEnabled

  override def error(msg: String): Unit = logger.error(msg)

  override def error(msg: String, cause: Throwable): Unit = logger.error(cause)(msg)

  override def debug(msg: String): Unit = logger.debug(msg)

  override def debug(msg: String, cause: Throwable): Unit = logger.debug(cause)(msg)

  override def isWarnEnabled: Boolean = logger.isWarnEnabled

  override def info(msg: String): Unit = logger.info(msg)

  override def info(msg: String, cause: Throwable): Unit = logger.info(cause)(msg)
}