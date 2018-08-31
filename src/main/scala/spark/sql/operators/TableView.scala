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

package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.sql.SqlQuery

/**
  * Given the [[viewName]] of a temporary table view, infers the
  * [[SqlBindingTableMetadata.schemaMap]] and [[SqlBindingTableMetadata.btableSchema]] from the
  * table schema yielded by the DESCRIBE TABLE command in Spark.
  */
case class TableView(viewName: String, sparkSession: SparkSession)
  extends target_api.TableView(viewName) {

  override val bindingTable: BindingTableMetadata = {

    val globalView: String = s"global_temp.$viewName"

    // Infer binding table's schema from its global view.
    val viewSchema: DataFrame = sparkSession.sql(s"DESCRIBE TABLE $globalView")
    var schema: StructType = new StructType()
    viewSchema
      .collect()
      .foreach(attributes =>
        schema = schema.add(attributes(0).asInstanceOf[String], attributes(1).asInstanceOf[String]))

    val schemaMap: Map[Reference, StructType] =
      schema
        .map(field => {
          val fieldNameTokens: Array[String] = field.name.split("\\$")
          val refName: String = fieldNameTokens.head
          Reference(refName) -> field
        })
        .groupBy(_._1)
        .map {
          case (ref, seqOfTuples) => (ref, StructType(seqOfTuples.map(_._2)))
        }

    SqlBindingTableMetadata(
      sparkSchemaMap = schemaMap,
      sparkBtableSchema = schema,
      btableOps = SqlQuery(resQuery = globalView)
    )
  }
}
