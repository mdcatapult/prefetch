package io.mdcatapult.doclib.remote

package object adapters {
  val allProtocols = Http.protocols ::: Ftp.protocols
}
