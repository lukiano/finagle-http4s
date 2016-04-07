package org.http4s
package finagle

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.security.{ KeyStore, Security }
import java.util.concurrent.{ ConcurrentLinkedQueue, ExecutorService }
import javax.net.ssl.{ KeyManagerFactory, SSLContext, TrustManagerFactory }

import com.codahale.metrics.MetricRegistry
import com.twitter.concurrent.{ BridgedThreadPoolScheduler, Scheduler }
import com.twitter.finagle.builder.{ ServerBuilder => FinagleBuilder }
import com.twitter.finagle.http.{ Request => FinagleRequest, Response => FinagleResponse }
import com.twitter.finagle.stats.{ DefaultStatsReceiver, StatsReceiver }
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finagle.{ ListeningServer, Service => FinagleService, ServiceFactory }
import com.twitter.util.{ Await, Future, Duration => TwitterDuration }
import org.http4s.server._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scalaz.concurrent.Task
import scalaz.syntax.std.option._

import FinagleConverters._

case class FinagleServerBuilder(
    executorService: Option[ExecutorService] = None,
    services: List[(String, HttpService)] = Nil,
    socketAddress: InetSocketAddress = ServerBuilder.DefaultSocketAddress,
    metricRegistry: Option[MetricRegistry] = None,
    metricPrefix: Option[String] = None,
    idleTimeout: Duration = IdleTimeoutSupport.DefaultIdleTimeout,
    sslBits: Option[SSLSupport.SSLBits] = None
) extends ServerBuilder with MetricsSupport with IdleTimeoutSupport with SSLSupport {

  override type Self = FinagleServerBuilder

  override def withServiceExecutor(executorService: ExecutorService) = copy(executorService = Some(executorService))

  override def mountService(service: HttpService, prefix: String) = copy(services = (prefix, service) :: services)

  override def bindSocketAddress(socketAddress: InetSocketAddress) = copy(socketAddress = socketAddress)

  override def withMetricRegistry(metricRegistry: MetricRegistry) = copy(metricRegistry = Some(metricRegistry))

  override def withMetricPrefix(metricPrefix: String) = copy(metricPrefix = Some(metricPrefix))

  override def withIdleTimeout(idleTimeout: Duration): FinagleServerBuilder = copy(idleTimeout = idleTimeout)

  override def withSSL(keyStore: SSLSupport.StoreInfo, keyManagerPassword: String, protocol: String, trustStore: Option[SSLSupport.StoreInfo], clientAuth: Boolean) =
    copy(sslBits = Some(SSLSupport.SSLBits(keyStore, keyManagerPassword, protocol, trustStore, clientAuth)))

  override def start: Task[Server] = Task.delay {
    NettyLogger()
    val finagleLogger = com.twitter.finagle.util.DefaultLogger
    finagleLogger.setLevel(java.util.logging.Level.ALL)
    finagleLogger.setUseParentHandlers(false)
    finagleLogger.addHandler(new org.slf4j.bridge.SLF4JBridgeHandler())
    executorService.foreach { es => Scheduler.setUnsafe(new BridgedThreadPoolScheduler("custom", _ => es)) }
    new Server {
      private val aggregateService = server.Router(services.reverse: _*)

      private val serviceFactory: ServiceFactory[FinagleRequest, FinagleResponse] = ServiceFactory(() => Future {
        FinagleService.mk {
          aggregateService.dimap(request.to, response.from).mapK(_.asFuture()).run
        }
      })

      private val shutdownHooks = new ConcurrentLinkedQueue[() => Unit]()

      private val stats: StatsReceiver = metricRegistry.cata(
        registry => new CHMetricsStatsReceiver(registry, metricPrefix getOrElse ""), DefaultStatsReceiver
      )

      private val builder = FinagleBuilder()
        .codec(new ServerCodec(stats).server)
        .bindTo(socketAddress)
        .name("Http4sFinagleServer")
        .readTimeout(TwitterDuration(idleTimeout.length, idleTimeout.unit))
        .reportTo(stats)

      getContext.foreach {
        case (ctx, clientAuth) =>
          builder.newSslEngine(() => {
            val eng = ctx.createSSLEngine()
            eng.setUseClientMode(false)
            eng.setNeedClientAuth(clientAuth)
            eng
          })
      }

      private val finagleServer: ListeningServer = builder.build(serviceFactory)

      private def getContext: Option[(SSLContext, Boolean)] = sslBits.map { bits =>
        val ksStream = new FileInputStream(bits.keyStore.path)
        val ks = KeyStore.getInstance("JKS")
        ks.load(ksStream, bits.keyStore.password.toCharArray)
        ksStream.close()

        val tmf = bits.trustStore.map { auth =>
          val ksStream = new FileInputStream(auth.path)

          val ks = KeyStore.getInstance("JKS")
          ks.load(ksStream, auth.password.toCharArray)
          ksStream.close()

          val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)

          tmf.init(ks)
          tmf.getTrustManagers
        }

        val kmf = KeyManagerFactory.getInstance(
          Option(Security.getProperty("ssl.KeyManagerFactory.algorithm"))
            .getOrElse(KeyManagerFactory.getDefaultAlgorithm)
        )

        kmf.init(ks, bits.keyManagerPassword.toCharArray)

        val context = SSLContext.getInstance(bits.protocol)
        context.init(kmf.getKeyManagers, tmf.orNull, null)

        (context, bits.clientAuth)
      }

      override def shutdown: Task[Unit] = finagleServer.close().asTask

      override def address: InetSocketAddress = InetSocketAddressUtil.toPublic(finagleServer.boundAddress).asInstanceOf[InetSocketAddress]

      override def onShutdown(f: => Unit) = {
        shutdownHooks.add(() => f)
        this
      }

      override def awaitShutdown(): Unit = Await.result(finagleServer)

      private val hook = new Thread(new Runnable {
        def run() = {
          awaitShutdown()
          shutdownHooks.asScala.foreach(_())
        }
      })
      hook.setDaemon(true)
      hook.setName("Http4sFinagleServer-shutdown-hook-thread")
      hook.start()
    }
  }
}
