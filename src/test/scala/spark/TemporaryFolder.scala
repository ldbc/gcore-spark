/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
