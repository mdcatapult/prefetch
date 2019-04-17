package io.mdcatapult.doclib.util

import io.lemonlabs.uri.Uri
import play.api.libs.json._

import scala.util._

trait LemonLabsUriJson {

  implicit val lemonLabsUriReader: Reads[Uri] = (jv: JsValue) =>
    Uri.parseTry(jv.toString()) match {
      case Success(u) ⇒ JsSuccess(u)
      case Failure(e) ⇒ JsError(e.getMessage)
    }

  implicit val lemonLabsUriWriter: Writes[Uri] = (uri: Uri) => JsString(uri.toString())
  implicit val lemonLabsUriFormatter: Format[Uri] = Format(lemonLabsUriReader, lemonLabsUriWriter)
}
