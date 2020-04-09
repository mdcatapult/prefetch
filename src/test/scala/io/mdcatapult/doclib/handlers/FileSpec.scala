package io.mdcatapult.doclib.handlers

import java.io.File

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileSpec extends AnyFlatSpec with Matchers {

  "A file" should "list files when not a directory" in {
    val f = File.createTempFile("deletion-test", "txt")
    f.deleteOnExit()

    Option(f.listFiles()) should be(None)
  }
}
