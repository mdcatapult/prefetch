package io.mdcatapult.doclib.util

import java.time.{LocalDateTime, ZoneOffset}

import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class LocalDateTimeCodec extends Codec[LocalDateTime] {

  override def decode(bsonReader: BsonReader, decoderContext: DecoderContext): LocalDateTime =
    LocalDateTime.ofEpochSecond(bsonReader.readDateTime(), 0, ZoneOffset.UTC)

  override def encode(bsonWriter: BsonWriter, t: LocalDateTime, encoderContext: EncoderContext): Unit =
    bsonWriter.writeDateTime(t.toInstant(ZoneOffset.UTC).toEpochMilli)

  override def getEncoderClass: Class[LocalDateTime] = classOf[LocalDateTime]
}