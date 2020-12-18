package io.mdcatapult.doclib.prefetch.model

import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.messages.PrefetchMsg
import io.mdcatapult.doclib.models.{DoclibDoc, Origin}

object Exceptions {

  /**
   * If a file has zero length then prefetch will not process it during ingest.
   *
   * @param filePath The path to the zero length file
   * @param doc      The mongodb record which references this file
   */
  class ZeroLengthFileException(filePath: String, doc: DoclibDoc) extends Exception(s"$filePath has zero length and will not be processed further. See doclib record ${doc._id} for more details.")

  /**
   * If a file has zero length then prefetch will not process it during ingest.
   *
   * @param doc The mongodb record which references this file
   */
  class SilentValidationException(doc: DoclibDoc) extends DoclibDocException(doc, "Suppressed exception for Validation")

  class InvalidOriginSchemeException(msg: PrefetchMsg) extends Exception(s"$msg contains invalid origin scheme")

  class MissingOriginSchemeException(msg: PrefetchMsg, origin: Origin) extends Exception(s"$origin has no uri: msg=$msg")

}
