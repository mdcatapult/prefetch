package io.mdcatapult.doclib.util

import io.lemonlabs.uri.AbsoluteUrl
import org.bson._
import org.bson.codecs._

import scala.util._

class LemonLabsAbsoluteUrlCodec extends Codec[AbsoluteUrl] {

  override def decode(bsonReader: BsonReader, decoderContext: DecoderContext): AbsoluteUrl =
    AbsoluteUrl.parseTry(bsonReader.readString()) match {
      case Success(uri) ⇒ uri
      case Failure(e) ⇒ throw e
    }

  override def encode(bsonWriter: BsonWriter, t: AbsoluteUrl, encoderContext: EncoderContext): Unit =
    bsonWriter.writeString(t.toString())

  override def getEncoderClass: Class[AbsoluteUrl] = classOf[AbsoluteUrl]


}
