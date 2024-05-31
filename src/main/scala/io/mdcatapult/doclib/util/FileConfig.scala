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

import com.typesafe.config.Config

object FileConfig {

  case class SharedConfig(doclibRoot: String,
                          archiveDirName: String,
                          localDirName: String,
                          remoteDirName: String)

  def getSharedConfig(conf: Config): SharedConfig = {
    SharedConfig(
      doclibRoot = s"${conf.getString("doclib.root").replaceFirst("""/+$""", "")}/",
      archiveDirName = conf.getString("doclib.archive.target-dir"),
      localDirName = conf.getString("doclib.local.target-dir"),
      remoteDirName = conf.getString("doclib.remote.target-dir")
    )
  }
}
