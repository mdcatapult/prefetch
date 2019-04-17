package io.mdcatapult.doclib.util

import io.lemonlabs.uri.Uri
import play.api.libs.json._

import scala.util._

trait LemonLabsUriJson {

  implicit val lemonLabsUriReader: Reads[Uri] = (jv: JsValue) =>
    Uri.parseTry(jv.asInstanceOf[JsString].value) match {
      case Success(u) ⇒ JsSuccess(u)
      case Failure(e) ⇒ JsError(e.getMessage)
    }

  implicit val lemonLabsUriWriter: Writes[Uri] = (uri: Uri) => {
    val u = uri.toString()
    JsString(u)
  }
  implicit val lemonLabsUriFormatter: Format[Uri] = Format(lemonLabsUriReader, lemonLabsUriWriter)
}
