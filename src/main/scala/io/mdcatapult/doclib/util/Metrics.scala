/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.util

import io.prometheus.client.Summary

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
