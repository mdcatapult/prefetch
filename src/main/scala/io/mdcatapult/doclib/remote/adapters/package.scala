package io.mdcatapult.doclib.remote
import scala.collection.immutable

package object adapters {
  val allProtocols: immutable.Seq[String] = Http.protocols ::: Ftp.protocols
}
