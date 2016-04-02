package org.http4s.finagle

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.twitter.finagle.stats.{Counter, Stat, StatsReceiver, Gauge => FGauge}

import scalaz.syntax.id._

class CHMetricsStatsReceiver(registry: MetricRegistry, prefix: String) extends StatsReceiver {
  override val repr: AnyRef = this

  override def counter(names: String*): Counter =
    registry.counter(format(names)) |> { c =>
      new Counter {
        override def incr(delta: Int): Unit = c.inc(delta.toLong)
      }
    }

  override def addGauge(names: String*)(f: => Float): FGauge =
    format(names) |> { name =>
      registry.register(format(names), new Gauge[Float] {
        override def getValue: Float = f
      }) |> { g =>
        new FGauge {
          override def remove(): Unit = { registry.remove(name); () }
        }
      }
    }

  override def stat(names: String*): Stat =
    registry.histogram(format(names)) |> { h =>
      new Stat {
        override def add(value: Float): Unit = h.update(value.toLong)
      }
    }

  private def format(names: Seq[String]): String =
    (prefix +: names).mkString(".")
}