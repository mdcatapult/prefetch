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
  class ZeroLengthFileException(filePath: String, doc: DoclibDoc) extends Exception(s"$filePath has zero length and will not be processed further. " +
    s"Doclib record ${doc._id} for this non-ingressed file has now been deleted." +
    s"The original file may still be available in the ingress folder. Remove it if you require.")

  /**
   * If a file has zero length then prefetch will not process it during ingest.
   *
   * @param doc The mongodb record which references this file
   */
  class SilentValidationException(doc: DoclibDoc) extends DoclibDocException(doc, "Suppressed exception for Validation")

  class InvalidOriginSchemeException(msg: PrefetchMsg) extends Exception(s"$msg contains invalid origin scheme")

  class MissingOriginSchemeException(msg: PrefetchMsg, origin: Origin) extends Exception(s"$origin has no uri: msg=$msg")

  class RogueFileException(msg: PrefetchMsg, source: String) extends  Exception(s"cannot process rogue file. Source=$source, msg=$msg")

}
