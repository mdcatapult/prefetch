package io.mdcatapult.doclib

import better.files.{File ⇒ ScalaFile}

/**
 * Convenience method to delete a list of directories
 */
trait TestDirectoryDelete  {

  /**
   * Delete a list of directories from the file system
   * @param directories List of directories to be deleted
   */
  def deleteDirectories(directories: List[ScalaFile]): Unit = {
    directories.map(_.delete(true))
  }

}
