package io.mdcatapult.doclib.remote.adapters

case class DoclibFtpRetrievalError(message: String, cause: Throwable = None.orNull) extends Exception(message, cause)
