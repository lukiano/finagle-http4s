Http4s wrapper for a Finagle server
===================================

Usage
-----

    import java.net.InetSocketAddress

    import com.codahale.metrics.MetricRegistry

    import org.http4s.{HttpService, Server}
    import org.http4s.finagle.FinagleServerBuilder

    import scala.concurrent.Duration
    
    import scalaz.concurrent.Task

    object Main extends App {

      def buildServer(): Task[Server] = {
        val address = new InetSocketAddress(8080)
        val service: HttpService = ???
        val metricRegistry: MetricRegistry = ???
        val idleTimeout: Duration = ???
  
        FinagleServerBuilder()
          .withMetricRegistry(metricRegistry)
          .withIdleTimeout(idleTimeout)
          .bindSocketAddress(address)
          .mountService(service)
          .start
      }
      
      buildServer().run
      
    }
