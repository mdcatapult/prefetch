package io.mdcatapult.doclib.remote

case class DownloadResult(
                           source: String,
                           origin: Option[String] = None
                         )
