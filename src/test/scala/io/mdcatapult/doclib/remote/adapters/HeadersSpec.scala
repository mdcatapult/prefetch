package io.mdcatapult.doclib.remote.adapters

import akka.http.javadsl.model.HttpHeader
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.OptionValues._

class HeadersSpec extends FlatSpec with Matchers with MockFactory {

  "An Headers.filename" should "find filename from Content-Disposition with filename*" in {
    val header = stub[HttpHeader]

    (header.lowercaseName _).when().returns("content-disposition")
    (header.value _).when().returns("attachment; filename*=\"DETERMINATION OF VP IN CRUDE_SM_VER.pdf\"")

    Headers.filename(Seq(header)).value should be("DETERMINATION OF VP IN CRUDE_SM_VER.pdf")
  }

  it should "find filename from Content-Disposition with filename (no *)" in {
    val header = stub[HttpHeader]

    (header.lowercaseName _).when().returns("content-disposition")
    (header.value _).when().returns("attachment; filename=\"cool.html\"")

    Headers.filename(Seq(header)).value should be("cool.html")
  }

  it should "prefer filename* over filename" in {
    val header = stub[HttpHeader]

    (header.lowercaseName _).when().returns("content-disposition")
    (header.value _).when().returns("attachment; filename*=\"coolio.pdf\"; filename=\"cool.pdf\"")

    Headers.filename(Seq(header)).value should be("coolio.pdf")
  }

  it should "prefer filename* over filename even if filename* is last" in {
    val header = stub[HttpHeader]

    (header.lowercaseName _).when().returns("content-disposition")
    (header.value _).when().returns("attachment; filename=\"cool.pdf\"; filename*=\"coolio.pdf\"")

    Headers.filename(Seq(header)).value should be("coolio.pdf")
  }

  it should "ignore header when it is not content-disposition" in {
    val header = stub[HttpHeader]

    (header.lowercaseName _).when().returns("content-type")
    (header.value _).when().returns("attachment; filename=\"cool.pdf\"; filename*=\"coolio.pdf\"")

    Headers.filename(Seq(header)) should be(None)
  }

  it should "find filename from a sequence of headers" in {
    val headers = Seq(stub[HttpHeader], stub[HttpHeader], stub[HttpHeader])

    (headers.head.lowercaseName _).when().returns("content-type")
    (headers.head.value _).when().returns("application/pdf")

    (headers(1).lowercaseName _).when().returns("content-disposition")
    (headers(1).value _).when().returns("attachment; filename*=\"DETERMINATION OF VP IN CRUDE_SM_VER.pdf\"")

    (headers.last.lowercaseName _).when().returns("content-length")
    (headers.last.value _).when().returns("133694")

    Headers.filename(headers).value should be("DETERMINATION OF VP IN CRUDE_SM_VER.pdf")
  }

  it should "find no filename from an empty sequence" in {
    Headers.filename(Seq()) should be(None)
  }
}
