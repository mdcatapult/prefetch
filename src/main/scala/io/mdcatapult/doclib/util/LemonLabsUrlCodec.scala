package io.mdcatapult.doclib.util

import io.lemonlabs.uri.Url
import org.bson._
import org.bson.codecs._
import scala.util._

class LemonLabsUrlCodec extends Codec[Url] {

  override def decode(bsonReader: BsonReader, decoderContext: DecoderContext): Url =
    Url.parseTry(bsonReader.readString()) match {
      case Success(uri) ⇒ uri
      case Failure(e) ⇒ throw e
    }

  override def encode(bsonWriter: BsonWriter, t: Url, encoderContext: EncoderContext): Unit =
    bsonWriter.writeString(t.toString())

  override def getEncoderClass: Class[Url] = classOf[Url]


}
