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

import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.StructType
import spark.sql.SqlQuery
import SqlQuery.mergeSchemas
import common.RandomNameGenerator.randomString

/** A generic join of two relations: [[lhs]] JOIN [[rhs]]. */
abstract class Join(lhs: TargetTreeNode, rhs: TargetTreeNode) extends target_api.Join(lhs, rhs) {

  /** The SQL keyword for this [[Join]]. Example: INNER JOIN, CROSS JOIN, etc. */
  def joinTypeSql: String

  /**
    * The condition on which to join. Must be expressed as a valid SQL join condition. Example:
    * ON lhs.id = rhs.id, USING (id), etc.
    */
  def joinCondition: String

  val lhsBtable: SqlBindingTableMetadata = lhs.bindingTable.asInstanceOf[SqlBindingTableMetadata]
  val rhsBtable: SqlBindingTableMetadata = rhs.bindingTable.asInstanceOf[SqlBindingTableMetadata]
  val lhsSchema: StructType = lhsBtable.btableSchema
  val rhsSchema: StructType = rhsBtable.btableSchema

  var lhsAlias: String = randomString()
  var rhsAlias: String = randomString()


  override val bindingTable: BindingTableMetadata = {
    val mergedSchemas: StructType = mergeSchemas(lhsSchema, rhsSchema)

    val joinQuery: String =
      s"""
      SELECT $commonCols FROM (${lhsBtable.btable.resQuery}) $lhsAlias
      $joinTypeSql (${rhsBtable.btable.resQuery}) $rhsAlias
      $joinCondition"""

    val sqlJoinQuery: SqlQuery = SqlQuery(resQuery = joinQuery)

    val unifiedSchema: StructType = new StructType(mergedSchemas.toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = lhsBtable.schemaMap ++ rhsBtable.schemaMap,
      sparkBtableSchema = unifiedSchema,
      btableOps = sqlJoinQuery)
  }

  def commonCols : String ={
    val leftTable = s"${lhsSchema.map(f => s"$lhsAlias.`${f.name}`").mkString(" , ")}"
    val rightTable =  s"${rhsSchema.diff(lhsSchema).map(f => s"$rhsAlias.`${f.name}`").mkString(" , ")}"

    if (rightTable.length>0)
      leftTable+","+rightTable
    else
      leftTable
  }
}
