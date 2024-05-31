/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.util

import io.lemonlabs.uri.Uri
import io.mdcatapult.util.hash.Md5.md5
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileHashSpec extends AnyFlatSpec with Matchers {

  "A filename that is shorter than 255 chars" should "not be changed" in {
    val origUri = Uri.parse("http://eprints.hud.ac.uk/10819/1/MorralNew.pdf")
    val fileName = "MorralNew.pdf"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)

    hashName should be (fileName)
  }

  "A uri with calculated filename that is longer than 255 chars with query params" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04).pdf?nofollow=true&rendering=author")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04).74bdbe0d010151d2d42f6768eca1290c.pdf"
    val actualName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)
    val fileNameHash = md5(actualName)

    hashName should be (s"$fileNameHash.74bdbe0d010151d2d42f6768eca1290c.pdf")
  }

  "A uri with filename longer than 255 chars with no query params" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be.pdf")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be.pdf"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)
    val fileNameHash = md5(fileName.replace(".pdf", ""))

    hashName should be (s"$fileNameHash.pdf")
  }

  "A uri with filename longer than 255 chars with no query params and no extension" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)
    val fileNameHash = md5(fileName)

    hashName should be (s"$fileNameHash")
  }

  "A url that has no path" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk")
    val fileName = ""
    val hashName = FileHash.hashOrOriginal(origUri, fileName)

    hashName should be (fileName)
  }

  "A url with a filename that is shorter than 255 chars and has a path" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk")
    val fileName = ""
    val hashName = FileHash.hashOrOriginal(origUri, fileName)

    hashName should be (fileName)
  }

  "A url filename that is shorter than 255 chars and has path" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk/news")
    val fileName = "news"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)

    hashName should be (fileName)
  }

  "A url filename that is shorter than 255 chars and has a path and query params" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk/news?a=b")
    // Note this not the actual query hash, just representative
    val fileName = "news.74bdbe0d010151d2d42f6768eca1290c"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)

    hashName should be (fileName)
  }

  "A url filename that is shorter than 255 chars and has a path, query params and extension" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk/news.pdf?a=b")
    // Note this not the actual query hash, just representative
    val fileName = "news.74bdbe0d010151d2d42f6768eca1290c.pdf"
    val hashName = FileHash.hashOrOriginal(origUri, fileName)

    hashName should be (fileName)
  }

}
