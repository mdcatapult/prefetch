/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
