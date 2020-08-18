package io.mdcatapult.doclib.handlers

import java.net.InetSocketAddress

import com.sun.net.httpserver.HttpServer
import com.typesafe.config.Config
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.HTTPServer

class AdminServer(config: Config, checkHealth: () => Boolean) {

  def this(config: Config) = this(config, () => true)

  def start(): Unit = {
    val port = config.getInt("admin.port")
    val addr = new InetSocketAddress(port)
    val srv = HttpServer.create(addr, 3)
    val healthCheckHandler = new HTTPHealthCheckHandler(checkHealth)
    srv.createContext("/health", healthCheckHandler)
    val reg = CollectorRegistry.defaultRegistry
    new HTTPServer(srv, reg, false)
  }
}

