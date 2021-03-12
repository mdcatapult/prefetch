package io.mdcatapult.doclib.handlers

import io.mdcatapult.doclib.models.{DoclibDoc, Origin}
import io.mdcatapult.doclib.remote.DownloadResult

case class FoundDoc(doc: DoclibDoc, archiveable: List[DoclibDoc] = Nil, origins: List[Origin] = Nil, download: Option[DownloadResult] = None)
