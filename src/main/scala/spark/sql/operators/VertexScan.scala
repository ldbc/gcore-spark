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

import algebra.operators.Column._
import algebra.operators.VertexRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import schema.{Catalog, Table}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

/**
  * Creates the table that will hold information about a vertex. The use-case is a MATCH query of
  * the type MATCH (a). Edge endpoints are <b>not</b> scanned with the [[VertexScan]] operator.
  */
case class VertexScan(vertexRelation: VertexRelation, graph: Graph, catalog: Catalog)
  extends target_api.VertexScan(vertexRelation, graph, catalog) {

  private val physTable: Table[DataFrame] =
    physGraph.tableMap(tableName).asInstanceOf[Table[DataFrame]]

  private val sqlQuery: SqlQuery = {
    physTable.data.createOrReplaceGlobalTempView(tableName.value)
    val scanQuery: String =
      s"""
      SELECT
      "${tableName.value}" AS `${binding.refName}$$${TABLE_LABEL_COL.columnName}`,
      ${selectAllPrependRef(physTable.data, binding)}
      FROM global_temp.${tableName.value}"""
    SqlQuery(resQuery = scanQuery)
  }

  private val schema: StructType = refactorScanSchema(physTable.data.schema, binding)

  override val bindingTable: BindingTableMetadata =
    SqlBindingTableMetadata(
      sparkSchemaMap = Map(binding -> schema),
      sparkBtableSchema = schema,
      btableOps = sqlQuery)
}
