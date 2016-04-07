package org.http4s
package finagle

import client.{ Client, DisposableResponse }
import com.twitter.finagle.{ Address, Name, Http, ServiceFactory }
import com.twitter.finagle.http.{ Request => FinagleRequest, Response => FinagleResponse }

import scalaz.concurrent.Task
import scalaz.syntax.monad._

object FinagleClient {
  def newClient(): Client = {
    val clients = new scala.collection.concurrent.TrieMap[Name.Bound, ServiceFactory[FinagleRequest, FinagleResponse]]
    def getAddress(request: Request): Address = {
      val port = request.uri.port orElse {
        request.uri.scheme map { _.value.toLowerCase } flatMap {
          case "http" => Some(80)
          case "https" => Some(443)
          case _ => None
        }
      } getOrElse 80
      Address(request.uri.host.get.value, port)
    }

    def service(request: Request): Task[DisposableResponse] =
      Task.suspend {
        (clients.getOrElseUpdate(Name.bound(getAddress(request)), Http.newClient(Name.bound(getAddress(request)), "")).apply() flatMap { service =>
          service(FinagleConverters.request.from(request)) map FinagleConverters.response.to map { DisposableResponse(_, Task.suspend(service.close().asTask)) }
        }) asTask
      }

    val shutdown: Task[Unit] =
      Task.suspend(Task.gatherUnordered(clients.values.toSeq.map { _.close().asTask }).void)

    Client(Service.lift(service), Task.suspend(shutdown))
  }
}
