package spark

import java.io.File
import java.util.UUID

import com.google.common.io.Files

trait TemporaryFolder {

  lazy val testDir: File = {
    val dir = Files.createTempDir()
    dir.mkdir
    dir
  }

  def newDir: File = {
    newDir(UUID.randomUUID.toString)
  }

  def newDir(name: String): File = {
    val f = new File(testDir.getPath + "/" + name)
    f.mkdirs
    f
  }

  def deleteTestDir(): Unit = deleteFile(testDir)

  private def deleteFile(f: File): Unit = {
    if (f isDirectory) {
      f.listFiles.foreach(deleteFile)
    }

    f delete
  }
}
