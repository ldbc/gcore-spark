/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - Universidad de Talca (2018)
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

import java.io.{File, IOException}
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.Catalog



class Directory {

  def loadDatabase(directory: String, sparkSession:SparkSession, catalog: Catalog): Boolean =
  {
    val dir = new File(directory)
    if (!dir.exists) {
      dir.mkdir
      return true
    }
    else
    {
      try {
        var subDirectories = getListOfSubDirectories(directory)
        subDirectories.foreach(subDirectory =>{
          loadGraph(directory+File.separator+subDirectory,subDirectory,sparkSession, catalog)
        })
        return true
      }
      catch{
        case _: Throwable => return false
      }
    }
  }

  def deleteGraph(graphName:String, database: String):Boolean = {
    var directory= database+File.separator+graphName
    try{
      FileUtils.deleteDirectory(new File(directory))
      true
    }
    catch {
      case ioe: IOException =>
        false
    }
  }


  private def loadGraph(subDirectory: String, graphName: String, sparkSession:SparkSession, catalog: Catalog)
  {
    var sparkCatalog : SparkCatalog = SparkCatalog(sparkSession)
    val graphSource = new GraphSource(sparkSession) {
      override val loadDataFn: String => DataFrame = _ => sparkSession.emptyDataFrame
    }
    sparkCatalog.registerGraph(graphSource,Paths.get(subDirectory+File.separator+"config.json"))
    catalog.registerGraph(sparkCatalog.graph(graphName))
  }

  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }



}
