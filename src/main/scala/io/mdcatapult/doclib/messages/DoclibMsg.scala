package io.mdcatapult.doclib.messages

import io.mdcatapult.klein.queue.Envelope
import play.api.libs.json.{Format, Json, Reads, Writes}

object DoclibMsg {
  implicit val msgReader: Reads[DoclibMsg] = Json.reads[DoclibMsg]
  implicit val msgWriter: Writes[DoclibMsg] = Json.writes[DoclibMsg]
  implicit val msgFormatter: Format[DoclibMsg] = Json.format[DoclibMsg]
}

case class DoclibMsg(id: String) extends Envelope
