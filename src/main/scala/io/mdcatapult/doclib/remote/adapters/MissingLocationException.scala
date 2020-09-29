package io.mdcatapult.doclib.remote.adapters

import io.lemonlabs.uri.Path
import io.mdcatapult.doclib.models.Origin

class MissingLocationException(val path: Path, val origin:Origin) extends Exception {

  override def toString: String = s"Location header was available but empty using $path and $origin"

}
