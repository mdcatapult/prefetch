package io.mdcatapult.doclib.prefetch.model

import io.mdcatapult.doclib.prefetch.model.Exceptions.SilentValidationException

case class SilentValidationExceptionWrapper(silentValidationException: SilentValidationException)
  extends PrefetchResultContainer