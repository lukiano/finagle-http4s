package org.http4s
package finagle

import client.{ Client, DisposableResponse }
import com.twitter.finagle.{ Address, Name, Http, ServiceFactory }
import com.twitter.finagle.http.{ Request => FinagleRequest, Response => FinagleResponse }

import scala.collection.concurrent.TrieMap
import scalaz.concurrent.Task
import scalaz.syntax.monad._
import FinagleConverters._

object FinagleClient {

  def getAddress(req: Request): Name.Bound = {
    val port = req.uri.port orElse {
      req.uri.scheme map { _.value.toLowerCase } flatMap {
        case "http"  => Some(80)
        case "https" => Some(443)
        case _       => None
      }
    } getOrElse 80
    Name.bound(Address(req.uri.host.get.value, port))
  }

  def apply(): Client = {
    val clients = new TrieMap[Name.Bound, ServiceFactory[FinagleRequest, FinagleResponse]]

    def getClient(name: Name.Bound): ServiceFactory[FinagleRequest, FinagleResponse] =
      clients.getOrElseUpdate(name, Http.newClient(name, ""))

    def service(req: Request): Task[DisposableResponse] = Task.suspend {
      val fResponse = for {
        service <- getClient(getAddress(req))()
        fResponse <- service(request.from(req))
        rep = response.to(fResponse)
      } yield DisposableResponse(rep, Task.suspend(service.close().asTask))
      fResponse.asTask
    }

    val shutdown: Task[Unit] = Task.suspend {
      Task.gatherUnordered(clients.values.toSeq map {
        _.close().asTask
      }).void
    }

    Client(Service.lift(service), Task.suspend(shutdown))
  }

}
