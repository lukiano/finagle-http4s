package com.twitter.finagle.netty4

import com.twitter.finagle.param.{ Stats, Timer }
import com.twitter.finagle.server.{ Listener, StackServer, StdStackServer }
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ Service, ServiceFactory, Stack }
import com.twitter.util.{ Closable, Time }
import io.netty.handler.codec.http.HttpServerCodec

// The Finagle-Netty project doesn't provide one of this yet.
case class Netty4Server[Req1, Rep1](
    bufferSize:        Int,
    dispatcherBuilder: (Transport[Any, Any], Service[Req1, Rep1]) => Closable,
    stack:             Stack[ServiceFactory[Req1, Rep1]]                      = StackServer.newStack[Req1, Rep1],
    params:            Stack.Params                                           = Stack.Params.empty
) extends StdStackServer[Req1, Rep1, Netty4Server[Req1, Rep1]] {

  protected type In = Any
  protected type Out = Any

  protected def copy1(
    stack:  Stack[ServiceFactory[Req1, Rep1]] = this.stack,
    params: Stack.Params                      = this.params
  ) = copy(stack = stack, params = params)

  protected def newListener(): Listener[Any, Any] =
    Netty4Listener(params + PipelineInit(cf => {
      cf.addLast("HttpServerCodec", new HttpServerCodec(4096, 8192, bufferSize))
      ()
    }))

  protected def newDispatcher(transport: Transport[In, Out], service: Service[Req1, Rep1]) = {
    // TODO: Expiration logic should be installed using ExpiringService
    // in StackServer#newStack. Then we can thread through "closes"
    // via ClientConnection.
    val Timer(timer) = params[Timer]
    val ExpiringService.Param(idleTime, lifeTime) = params[ExpiringService.Param]
    val Stats(sr) = params[Stats]
    val idle = if (idleTime.isFinite) Some(idleTime) else None
    val life = if (lifeTime.isFinite) Some(lifeTime) else None
    val dispatcher = dispatcherBuilder(transport, service)
    (idle, life) match {
      case (None, None) => dispatcher
      case _ =>
        new ExpiringService(service, idle, life, timer, sr.scope("expired")) {
          protected def onExpire() {
            dispatcher.close(Time.now)
            ()
          }
        }
    }
  }
}
