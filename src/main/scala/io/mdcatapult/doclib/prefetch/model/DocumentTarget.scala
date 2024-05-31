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

import io.mdcatapult.doclib.models.Origin

/**
 * Details about where a document came from and where it ended up in the doclib root
 * 
 * @param targetPath Location in doclib root that document should be moved to
 * @param correctLocation Is the document in its final place in the doclib root
 * @param source Where the document was prefetched from. This comes from the initial rabbit message
 * @param origins All the remote locations that the document has been fetched from including redirects
 */
case class DocumentTarget(targetPath: Option[String], correctLocation: Boolean, source: String, origins: List[Origin])
