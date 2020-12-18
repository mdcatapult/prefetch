package io.mdcatapult.doclib.prefetch.model

// This is needed due to type erasure at runtime, and is preferable to previous Either implementation
trait PrefetchResultContainer extends Product with Serializable
