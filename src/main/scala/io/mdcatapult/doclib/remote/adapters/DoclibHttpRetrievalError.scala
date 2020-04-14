package io.mdcatapult.doclib.remote.adapters

case class DoclibHttpRetrievalError(message: String, cause: Throwable = None.orNull) extends Exception(message, cause)
