package io.mdcatapult.doclib.handlers

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.net.HttpURLConnection

import com.sun.net.httpserver.{HttpExchange, HttpHandler}

class HTTPHealthCheckHandler(checkHealth: () => Boolean) extends HttpHandler {
  override def handle(exchange: HttpExchange): Unit = {
    val isHealthy = checkHealth()

    val response = new ByteArrayOutputStream()
    val writer = new OutputStreamWriter(response)
    if (isHealthy) writer.write("\"OK\"") else writer.write("\"unhealthy\"")

    writer.flush()
    writer.close()
    response.flush()
    response.close()

    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.getResponseHeaders.set("Content-Length", String.valueOf(response.size))

    if (isHealthy)
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.size)
    else
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.size)

    response.writeTo(exchange.getResponseBody)
    exchange.close()
  }
}
