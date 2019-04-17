package io.mdcatapult.doclib.messages

import io.mdcatapult.klein.queue.Envelope
import play.api.libs.json._
import play.api.libs.json.Json._
import io.mdcatapult.doclib.models.PrefetchOrigin
import io.mdcatapult.doclib.util.StringAnyMapJson

object PrefetchMsg extends StringAnyMapJson {
  implicit val msgReader: Reads[PrefetchMsg] = Json.reads[PrefetchMsg]
  implicit val msgWriter: Writes[PrefetchMsg] = Json.writes[PrefetchMsg]
  implicit val msgFormatter: Format[PrefetchMsg] = Json.format[PrefetchMsg]
}

case class PrefetchMsg(
  source: String,
  origin: Option[List[PrefetchOrigin]],
  tags: Option[List[String]],
  metadata: Option[Map[String, Any]]
) extends Envelope


