package io.mdcatapult.doclib.remote

case class DownloadResult(
                           source: String,
                           hash: String,
                           origin: Option[String] = None,
                           target: Option[String] = None
                         )
