package io.mdcatapult.doclib.util

import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._
import spray.json.JsArray

import scala.util._

trait StringAnyMapJson {

  implicit val stringAnyMapReader: Reads[Map[String, Any]] = (jv: JsValue) =>
    JsSuccess(jv match {
      case JsObject(fields) ⇒ fields.toMap[String, JsValue].map { case (k, v) => k → (v match {
        case JsBoolean(b) ⇒ b
        case JsString(s) ⇒ s
        case JsNumber(n) ⇒ Try(n.toBigIntExact()) match {
          case Success(value) ⇒ value.get.toLong
          case Failure(_) ⇒ n.toDouble
        }
        case _ ⇒ throw new IllegalArgumentException("Unable to convert value")
      }) }
      case _ ⇒ jv.as[Map[String, Any]].map { case (k, v) => k → v }
    })
  implicit val stringAnyMapWriter: Writes[Map[String, Any]] = (map: Map[String, Any]) =>
    Json.obj(map.map { case (s, o) =>
      val v: JsValueWrapper = o match {
        case b: Boolean ⇒ JsBoolean(b)
        case s: String ⇒ JsString(s)
        case n: Int ⇒ JsNumber(n)
        case n: Double ⇒ JsNumber(n)
        case n: Float ⇒ JsNumber(n.toDouble)
      }
      val ret: (String, JsValueWrapper) = s -> v
      ret
    }.toSeq: _*)
  implicit val stringAnyMapFormatter: Format[Map[String, Any]] = Format(stringAnyMapReader, stringAnyMapWriter)

}
