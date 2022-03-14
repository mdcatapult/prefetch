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
