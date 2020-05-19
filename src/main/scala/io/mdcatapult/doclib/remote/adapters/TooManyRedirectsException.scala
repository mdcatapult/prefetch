package io.mdcatapult.doclib.remote.adapters

import io.lemonlabs.uri.Uri

class TooManyRedirectsException(val uris: List[Uri]) extends RuntimeException {

  def addUri(u: Uri): TooManyRedirectsException = new TooManyRedirectsException(u :: uris)
}
