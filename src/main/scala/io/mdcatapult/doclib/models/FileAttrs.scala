package io.mdcatapult.doclib.models

import java.nio.file.attribute.FileTime
import java.time.LocalDateTime
import java.util.Date

import play.api.libs.json.{Format, Json, Reads, Writes}

object FileAttrs {
  implicit val msgReader: Reads[FileAttrs] = Json.reads[FileAttrs]
  implicit val msgWriter: Writes[FileAttrs] = Json.writes[FileAttrs]
  implicit val msgFormatter: Format[FileAttrs] = Json.format[FileAttrs]
}

case class FileAttrs(
                     path: String,
                     name: String,
                     mtime: LocalDateTime,
                     ctime: LocalDateTime,
                     atime: LocalDateTime,
                     size: Long,
                    )