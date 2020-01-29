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

import algebra.expressions.PropertyKey
import algebra.types._
import common.exceptions.UnsupportedOperation
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{parse, pretty}
import org.json4s.jackson.Serialization.write
import schema._

import scala.collection.mutable

/**
  * A [[PathPropertyGraph]] that uses Spark's [[DataFrame]]s to store graph data. Each tableName in the
  * data model of the graph is backed by a [[DataFrame]]. The [[GraphSchema]] is inferred from the
  * structure of each [[DataFrame]], so an extending class should typically define (in its
  * particular way) the [[schema.GraphData.vertexData]], [[schema.GraphData.edgeData]] and
  * [[schema.GraphData.pathData]].
  */
abstract class SparkGraph extends PathPropertyGraph {

  override type StorageType = DataFrame

  override def vertexSchema: EntitySchema = buildSchema(vertexData)

  override def edgeSchema: EntitySchema = buildSchema(edgeData)

  override def pathSchema: EntitySchema = buildSchema(pathData)

  def sparkSchemaString: String =
    s"\nGraph: $graphName\n" +
      s"[*] Vertex schema:\n$vertexData\n" +
      s"[*] Edge schema:\n$edgeData\n" +
      s"[*] Path schema:\n$pathData"

  /**
    * Infers the schema of an entity type (vertex, edge, path) from the sequence of data [[Table]]s
    * comprising this graph.
    */
  private def buildSchema(data: Seq[Table[DataFrame]]): EntitySchema =
    data.foldLeft(EntitySchema.empty) {
      case (aggSchema, table) =>
        val schemaFields = table.data.schema.fields
        val schemaMap = schemaFields.foldLeft(SchemaMap.empty[PropertyKey, GcoreDataType]) {
          case (aggMap, field) =>
            aggMap union SchemaMap(Map(PropertyKey(field.name) -> convertType(field.dataType)))
        }

        aggSchema union EntitySchema(SchemaMap(Map(table.name -> schemaMap)))
    }


  val nodes: mutable.HashMap[String, String] = new mutable.HashMap[String,String]()

  def vertexYARSPG(): String =
  {
    nodes.clear()
    var yarspg =""
    this.vertexData foreach(x =>
    {
      var dataframe= x.data.asInstanceOf[DataFrame]
      dataframe.toJSON.collect().foreach(att=> yarspg+=x.name+vertexAttributes(att,x.name.value)+"\n")
    })

    yarspg
  }

  def vertexAttributes(attributes:String,name:String):String=
  {
    implicit val formats = DefaultFormats
    var values = parse(attributes)
    var id= (values \ "id").extractOrElse("null")
    var values_minus_id = values.asInstanceOf[JObject].obj.filterNot(_._1 == "id")
    var json_atrrib = write(values_minus_id).replace("{","").replace("}","")
    var yarspg =""

    nodes += (id -> name)
    yarspg +=id+"{\""+name+"\"}"
    yarspg +=json_atrrib

    yarspg
  }


  def edgeYARSPG(): String =
  {
    var yarspg =""
    this.edgeData foreach(x =>
    {
      var dataframe= x.data.asInstanceOf[DataFrame]
      dataframe.toJSON.collect().foreach(att=> yarspg+= edgeAttributes(att,x.name.value)+"\n")
    })

    yarspg
  }

  def edgeAttributes(attributes:String, name:String):String=
  {
    implicit val formats = DefaultFormats
    var values = parse(attributes)
    var id= (values \ "id").extractOrElse("null")
    var fromId= (values \ "fromId").extractOrElse("null")
    var toId= (values \ "toId").extractOrElse("null")
    var values_minus_id = values.asInstanceOf[JObject].obj.filterNot(_._1 == "id").filterNot(_._1 == "fromId").filterNot(_._1 == "toId")
    var json_atrrib = write(values_minus_id).replace("{","").replace("}","")
    var yarspg ="("+nodes(fromId)+fromId+")-{\""+name+"\"}"+json_atrrib+"->("+nodes(toId)+toId+")"



    yarspg
  }


  def pathYARNSPG(): String =
  {
    var yarnspg =""
    this.pathData foreach(x =>
    {
      var dataframe= x.data.asInstanceOf[DataFrame]
      dataframe.toJSON.collect().foreach(att=> yarnspg+=("\n"+pathAttributes(att, x.name.value)))
    })
    yarnspg
  }

  def pathAttributes(attributes:String, name: String):String=
  {
    implicit val formats = DefaultFormats
    var values = parse(attributes)
    var id= (values \ "id").extract[String]
    var fromId= (values \ "fromId").extract[String]
    var toId= (values \ "toId").extract[String]
    var edges = (values \ "edges").extract[Seq[Int]]
    var values_minus_id = values.asInstanceOf[JObject].obj.filterNot(_._1 == "id").filterNot(_._1 == "fromId").filterNot(_._1 == "toId").filterNot(_._1 == "edges")
    var json_atrrib = write(values_minus_id).replace("{","").replace("}","")

    var yarnspg ="("+nodes(fromId)+fromId+")-/{\""+name+"\"}"+json_atrrib+"["+edges+"]"+"/->("+nodes(toId)+toId+")"


    yarnspg
  }

  /** Maps a [[spark.sql.types.DataType]] to an algebraic [[GcoreDataType]]. */
  // TODO: Check that the array type can only be the sequence of edges that define a path.
  private def convertType(sparkType: spark.sql.types.DataType): GcoreDataType = {
    sparkType.typeName match {
      case "string" => GcoreString
      case "integer" => GcoreInteger
      case "long" => GcoreInteger
      case "double" => GcoreDecimal
      case "float" => GcoreDecimal
      case "boolean" => GcoreBoolean
      case "array" => GcoreArray
      case "date" => GcoreDate
      case "timestamp" => GcoreTimestamp
      case other => throw UnsupportedOperation(s"Unsupported type $other")
    }
  }
}
