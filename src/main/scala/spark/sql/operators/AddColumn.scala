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
import algebra.operators.Column.{CONSTRUCT_ID_COL, ID_COL}
import algebra.target_api
import algebra.target_api.TargetTreeNode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.{SqlPlanner, SqlQuery}

case class AddColumn(reference: Reference, relation: TargetTreeNode)
  extends target_api.AddColumn(reference, relation) {

  override val bindingTable: target_api.BindingTableMetadata = {
    val relationBtable: SqlBindingTableMetadata =
      relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val relationSchemaMap: Map[Reference, StructType] = relationBtable.schemaMap

    val idColumnName: String = s"${reference.refName}$$${ID_COL.columnName}"
    val monotonicIdColumnName: String = s"${reference.refName}$$${CONSTRUCT_ID_COL.columnName}"

    // Add a monotic id to allow ordering by it in the next step.
    val addMonotonicIdQuery: String =
      s"""
      SELECT *, MONOTONICALLY_INCREASING_ID() AS `$monotonicIdColumnName`
      FROM (${relationBtable.btable.resQuery})"""

    // All columns in btable except for the monotonic id previously added.
    val allColumns: Seq[String] = relationBtable.btableSchema.map(field => s"`${field.name}`")
    val allColumnsSelect: String = allColumns.mkString(", ")
    val idSelect: String = s"ROW_NUMBER() OVER (ORDER BY `$monotonicIdColumnName`)"

    val addIdQuery: String =
      s"""
      SELECT $allColumnsSelect, concat('${reference.refName}',sid) AS `$idColumnName` FROM ($addMonotonicIdQuery)"""

    // If this variable needed an aggregation, it already has properties in the table so it will be
    // in schema. In this case, we only need to update its schema, otherwise we need to add it to
    // the binding table schema as a new entry.
    val idField = StructField(s"${reference.refName}$$${ID_COL.columnName}", IntegerType)
    val newRefSchema =
      StructType(relationSchemaMap.getOrElse(reference, new StructType()) :+ idField)
    val newSchemaMap = relationSchemaMap ++ Map(reference -> newRefSchema)
    val newBtableSchema = StructType(newSchemaMap.values.flatMap(_.fields).toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = newSchemaMap,
      sparkBtableSchema = newBtableSchema,
      btableOps = SqlQuery(resQuery = addIdQuery))
  }
}
