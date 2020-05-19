package io.mdcatapult.doclib.remote

final class UnableToFollow(
                            source: String,
                            cause: Throwable = None.orNull
                          )
  extends RuntimeException(
    s"Cannot follow redirect for '$source' as no valid Location header present",
    cause)
