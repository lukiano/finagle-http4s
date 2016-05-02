package com.twitter.finagle
package netty4

import com.twitter.finagle.http.{ Request, Response }

object Netty4Http {

  def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    http.Http.newClient(dest, label)

}
