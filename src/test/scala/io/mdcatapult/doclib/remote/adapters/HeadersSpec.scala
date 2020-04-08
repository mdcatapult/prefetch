package io.mdcatapult.doclib.remote.adapters

import akka.http.javadsl.model.HttpHeader
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HeadersSpec extends AnyFlatSpec with Matchers with MockFactory {

  "Headers.filename" should "find filename from Content-Disposition with filename*" in {
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

  it should "not find filename from a sequence of headers when Content-Disposition is not in list of headers" in {
    val headers = Seq(stub[HttpHeader], stub[HttpHeader], stub[HttpHeader])

    (headers.head.lowercaseName _).when().returns("content-type")
    (headers.head.value _).when().returns("application/pdf")

    (headers.last.lowercaseName _).when().returns("content-length")
    (headers.last.value _).when().returns("133694")

    Headers.filename(headers) should be(None)
  }

  it should "find no filename from an empty sequence" in {
    Headers.filename(Seq()) should be(None)
  }

  "Headers.contentType" should "find content type from header with Content-Type" in {
    val header = stub[HttpHeader]

    (header.lowercaseName _).when().returns("content-type")
    (header.value _).when().returns("application/pdf")

    Headers.contentType(Seq(header)).value should be("application/pdf")
  }

  it should "find content type from a sequence of headers" in {
    val headers = Seq(stub[HttpHeader], stub[HttpHeader], stub[HttpHeader])

    (headers(1).lowercaseName _).when().returns("content-disposition")
    (headers(1).value _).when().returns("attachment; filename*=\"DETERMINATION OF VP IN CRUDE_SM_VER.pdf\"")

    (headers.head.lowercaseName _).when().returns("content-type")
    (headers.head.value _).when().returns("text/html")

    (headers.last.lowercaseName _).when().returns("content-length")
    (headers.last.value _).when().returns("133694")

    Headers.contentType(headers).value should be("text/html")
  }

  it should "not find a content type from a sequence of headers when Content-Type is not in list of headers" in {
    val headers = Seq(stub[HttpHeader], stub[HttpHeader], stub[HttpHeader])

    (headers(1).lowercaseName _).when().returns("content-disposition")
    (headers(1).value _).when().returns("attachment; filename*=\"DETERMINATION OF VP IN CRUDE_SM_VER.pdf\"")

    (headers.last.lowercaseName _).when().returns("content-length")
    (headers.last.value _).when().returns("133694")

    Headers.contentType(headers) should be(None)
  }

  it should "find no content type from an empty sequence" in {
    Headers.contentType(Seq()) should be(None)
  }
}
