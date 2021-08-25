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

package schema
import java.io.File

import algebra.expressions.Label
import org.apache.spark.sql.DataFrame
import org.json4s.{DefaultFormats, JsonAST}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization.writePretty
/**
  * The path property graph is the queryable unit of G-CORE. The graph retains information about
  * vertices, edges and paths. The stored information refers to labels and key-value attributes
  * of an entity. A graph has a [[GraphSchema]], that describes its structure, and stored
  * [[GraphData]].
  */
abstract class PathPropertyGraph extends GraphSchema with GraphData {

  var graphName: String

  def isEmpty: Boolean =
    vertexData.isEmpty && edgeData.isEmpty && pathData.isEmpty

  def nonEmpty: Boolean = !isEmpty

  override def toString: String = schemaString

  def schemaString: String =
    s"\nGraph: $graphName\n" +
      s"[*] Vertex schema:\n$vertexSchema" +
      s"[*] Edge schema:\n$edgeSchema" +
      s"[*] Path schema:\n$pathSchema"

  def yarspg: String =
  {
    var  yarspg =
      vertexYARSPG()+"\n\n"+
      edgeYARSPG()+"\n\n"+
      pathYARNSPG()+"\n"


    implicit val formats = DefaultFormats
    //pretty(parse(json))
    yarspg

  }

  def vertexYARSPG(): String

  def vertexAttributes(attributes:String, name:String):String

  def edgeYARSPG(): String

  def edgeAttributes(attributes:String, name: String):String
  def pathYARNSPG(): String

  def pathAttributes(attributes:String, name: String):String
}

object PathPropertyGraph {

  val empty: PathPropertyGraph = new PathPropertyGraph {

    override var graphName: String = "PathPropertyGraph.empty"
    override def vertexData: Seq[Table[StorageType]] = Seq.empty
    override def edgeData: Seq[Table[StorageType]] = Seq.empty
    override def pathData: Seq[Table[StorageType]] = Seq.empty
    override def vertexSchema: EntitySchema = EntitySchema.empty
    override def pathSchema: EntitySchema = EntitySchema.empty
    override def edgeSchema: EntitySchema = EntitySchema.empty

    override def edgeRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
    override def storedPathRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty

    override def vertexYARSPG(): String = ""

    override def vertexAttributes(attributes: String,name: String): String = ""

    override def edgeYARSPG(): String = ""

    override def edgeAttributes(attributes: String, name: String): String = ""

    override def pathYARNSPG(): String = ""

    override def pathAttributes(attributes: String, name: String): String = ""
  }
}
