package io.mdcatapult.doclib.util

import io.prometheus.client.{Counter, Summary}

object Metrics {
  val documentSizeBytes: Summary = Summary.build()
    .name("document_size_bytes")
    .help("Summary of document size.")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .labelNames("scheme", "mimetype")
    .register()

  val documentFetchLatency: Summary = Summary.build()
    .name("document_fetch_latency")
    .help("Time taken to fetch documents in seconds.")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .labelNames("scheme")
    .register()

  val mongoLatency: Summary = Summary.build()
    .name("mongo_latency")
    .help("Time taken for a mongo request to return.")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .labelNames("operation")
    .register()

  val handlerCount: Counter = Counter.build()
    .name("handler_count")
    .help("Counts number of requests received by the handler.")
    .labelNames("source", "result")
    .register()
}
