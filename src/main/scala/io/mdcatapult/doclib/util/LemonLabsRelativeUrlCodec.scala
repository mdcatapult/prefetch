package io.mdcatapult.doclib.util

import io.lemonlabs.uri.RelativeUrl
import org.bson._
import org.bson.codecs._

import scala.util._

class LemonLabsRelativeUrlCodec extends Codec[RelativeUrl] {

  override def decode(bsonReader: BsonReader, decoderContext: DecoderContext): RelativeUrl =
    RelativeUrl.parseTry(bsonReader.readString()) match {
      case Success(uri) ⇒ uri
      case Failure(e) ⇒ throw e
    }

  override def encode(bsonWriter: BsonWriter, t: RelativeUrl, encoderContext: EncoderContext): Unit =
    bsonWriter.writeString(t.toString())

  override def getEncoderClass: Class[RelativeUrl] = classOf[RelativeUrl]


}
