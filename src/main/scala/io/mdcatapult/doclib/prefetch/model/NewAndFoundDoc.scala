package io.mdcatapult.doclib.prefetch.model

import io.mdcatapult.doclib.models.DoclibDoc

case class NewAndFoundDoc(doc: DoclibDoc, foundDoc: DoclibDoc)
  extends PrefetchResultContainer
