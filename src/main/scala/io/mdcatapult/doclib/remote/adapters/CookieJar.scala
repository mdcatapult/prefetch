package io.mdcatapult.doclib.remote.adapters

import java.net.{CookieManager, CookiePolicy}
import java.util.Collections.{emptyMap => emptyJavaMap}
import java.util.{HashMap => JHashMap, List => JList}

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair, `Set-Cookie`}
import io.lemonlabs.uri.Uri
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

trait CookieJar {

  /** Get Cookie headers to be sent with a request */
  def getCookies(uri: Uri): Seq[HttpHeader]

  /** Add Cookies into the "Cookie Jar". */
  def addCookies(uri: Uri, cookies: Seq[`Set-Cookie`]): CookieJar
}

object CookieJar {

  /** The default empty CookieJar that will honour any Cookies that are added. */
  val empty: CookieJar = new CookieJar() {
    override def getCookies(uri: Uri): Seq[HttpHeader] = Nil

    override def addCookies(uri: Uri, cookies: Seq[`Set-Cookie`]): CookieJar = {
      if (cookies.isEmpty)
        this
      else {
        val jar = new StoreBackedCookieJar()
        jar.addCookies(uri, cookies)
        jar
      }
    }
  }
}

/** A CookieJar implementation that keeps track of any added Cookie.
  * It uses the Java Cookie implementation that is somewhat flawed.  The full Cookie specification
  * includes a parameter SameSite that may be STRICT or LAX (https://tools.ietf.org/html/draft-ietf-httpbis-cookie-same-site-00).
  * At times via redirecting we need the LAX behaviour, which isn't directly supported by the Java Cookie API.  The
  * work-around is to accept all Cookies.
  */
class StoreBackedCookieJar extends CookieJar {

  private val sameSiteLaxSupportingPolicy = CookiePolicy.ACCEPT_ALL

  private val manager = new CookieManager(null, sameSiteLaxSupportingPolicy)

  override def addCookies(uri: Uri, cookies: Seq[`Set-Cookie`]): CookieJar = {
    val cookieValues = new ArrayListValuedHashMap[String,String]()
    cookies.foreach(cookie => cookieValues.put(cookie.name(), cookie.value()))

    val m = new JHashMap[String,JList[String]]()
    cookieValues.keySet().asScala.foreach(k => m.put(k, cookieValues.get(k)))

    manager.put(uri.toJavaURI, m)
    this
  }

  override def getCookies(uri: Uri): Seq[HttpHeader] = {
    val cookies = manager.get(uri.toJavaURI, emptyJavaMap()).asScala.view.mapValues(_.asScala.toSeq).toMap

    val pairs =
      for {
        keyValues <- Seq(cookies.toSeq:_*)
        key = keyValues._1
        value <- keyValues._2
      } yield HttpCookiePair(key, value)

    if (pairs.isEmpty)
      Nil
    else
      Seq(Cookie(pairs))
  }
}
