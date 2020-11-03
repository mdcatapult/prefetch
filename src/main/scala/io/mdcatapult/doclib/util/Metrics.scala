package io.mdcatapult.doclib.util

import io.prometheus.client.{Summary}

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

  val fileOperationLatency: Summary = Summary.build()
    .name("file_operation_latency")
    .help("Time taken to perform file operation in seconds.")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .labelNames("operation")
    .register()

}
