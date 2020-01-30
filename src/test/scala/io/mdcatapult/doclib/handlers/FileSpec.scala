package io.mdcatapult.doclib.handlers

import java.io.File

import org.scalatest.{FlatSpec, Matchers}

class FileSpec extends FlatSpec with Matchers {

  "A file" should "list files when not a directory" in {
    val f = File.createTempFile("deletion-test", "txt")
    f.deleteOnExit()

    Option(f.listFiles()) should be(None)
  }
}
