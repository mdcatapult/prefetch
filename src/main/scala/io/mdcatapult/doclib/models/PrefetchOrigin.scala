package io.mdcatapult.doclib.models

import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.util._
import play.api.libs.json.{Format, Json, Reads, Writes}

object PrefetchOrigin extends StringAnyMapJson with LemonLabsUriJson {
  implicit val prefetchOriginReader: Reads[PrefetchOrigin] = Json.reads[PrefetchOrigin]
  implicit val prefetchOriginWriter: Writes[PrefetchOrigin] = Json.writes[PrefetchOrigin]
  implicit val prefetchOriginFormatter: Format[PrefetchOrigin] = Json.format[PrefetchOrigin]
}

case class PrefetchOrigin(
  scheme: String,
  uri: Uri,
  headers: Option[Map[String, Seq[String]]] = None,
  metadata: Option[Map[String, Any]] = None
)



