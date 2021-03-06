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
import algebra.operators.Column._
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.SqlQuery

/**
  * Projects the [[attributes]] from the given [[relation]].
  *
  * An entity in the graph is uniquely represented by its id. The attributes to project are given as
  * a sequence of [[Reference]]s - the entity names. Thus, we distinguish between three types of
  * attributes in the context of the [[Project]] operator:
  *
  * (1) There is a set of columns already present in the [[relation]] that represents the
  * [[Reference]]'s properties. The set of columns also includes the [[ID_COL]]. In this case, the
  * result of the [[Project]]ion is the SQL SELECT-ion of the columns in the said set. This case
  * occurs when projecting a matched variable.
  *
  * (2) There is a set of columns already present in the [[relation]] that represents the
  * [[Reference]]'s properties. The set of columns does <b>not</b> include the [[ID_COL]]. In this
  * case, the result of the [[Project]]ion is the SQL SELECT-ion of the columns in the said set,
  * plus a new column aliased as the [[ID_COL]], filled in with MONOTONICALLY_INCREASING_IDs, to
  * ensure the id of each entity instance is uniquely keyed. This case occurs when projecting an
  * unmatched variable that contained grouping with aggregation. The aggregation is solved as a
  * temporary property of the variable, that is removed at [[VertexCreate]] or [[EdgeCreate]] level.
  * The aggregated property is added at the [[GroupBy]] level. The [[Project]]ion occurs between the
  * two.
  *
  * (3) There is no column in the [[relation]] to represent a property of the [[Reference]]'s. In
  * this case, the result of the [[Project]]ion is the SQL SELECT-ion of a column aliased as
  * [[ID_COL]] filled in with MONOTONICALLY_INCREASING_IDs, to ensure the id of each entity
  * instance is uniquely keyed. This case occurs when projecting an unmatched variable with no
  * grouping, or an unmatched variable with grouping, but no aggregation.
  */
case class Project(relation: TargetTreeNode, attributes: Seq[Reference])
  extends target_api.Project(relation, attributes) {

  override val bindingTable: BindingTableMetadata = {
    val relationBtable: SqlBindingTableMetadata =
      relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val relationSchemaMap: Map[Reference, StructType] = relationBtable.schemaMap

    val existingRefs: Seq[Reference] =
      attributes.filter(attr => relationSchemaMap.get(attr).isDefined)
    val existingColumnsSelect: Set[String] =
      existingRefs
        .flatMap(ref => relationSchemaMap(ref).fields)
        .map(field => s"`${field.name}`")
        .toSet

    val refsNotInSchema: Set[Reference] =
      attributes
        .filter(attr => relationSchemaMap.get(attr).isEmpty)
        .toSet
    val refsInSchemaWithoutId: Set[Reference] =
      existingRefs // in schema
        .filterNot(ref =>
          relationSchemaMap(ref)
            .fields
            .exists(_.name == s"${ref.refName}$$${ID_COL.columnName}")) // no id column
        .toSet
    val newRefs: Set[Reference] = refsNotInSchema ++ refsInSchemaWithoutId
    // For each new variable, we add a column containing monotonically increasing id's. We cannot
    // simply add any constant here, because it will be coalesced into a single group by a GROUP BY
    // clause. The monotonic id is actually one id per partition and, for each row on that partition
    // it increases by one. Therefore, in the resulting table, we may see very skewed id's in the
    // new columns. However, they should be distinct from each other and thus have all the new rows
    // preserved after the GROUP BY.
    val newRefsSelect: Set[String] =
      newRefs.map(
        ref => s"MONOTONICALLY_INCREASING_ID() AS `${ref.refName}$$${ID_COL.columnName}`")

    val columnsToSelect: Set[String] = existingColumnsSelect ++ newRefsSelect

    val sqlProject: String =
      s"""
      SELECT ${columnsToSelect.mkString(", ")} FROM (${relationBtable.btable.resQuery})"""

    val refsInSchemaWithIdRefSchema: Map[Reference, StructType] =
      existingRefs
        .filterNot(ref => refsInSchemaWithoutId.contains(ref))
        .map(ref => ref -> relationSchemaMap(ref))
        .toMap
    val refsInSchemaWithoutIdRefSchema: Map[Reference, StructType] =
      refsInSchemaWithoutId
        .map(ref => {
          val previousSchema: StructType = relationSchemaMap(ref)
          val newIdStructField: StructField =
            StructField(s"${ref.refName}$$${ID_COL.columnName}", IntegerType)
          ref -> StructType(previousSchema.fields :+ newIdStructField)
        })
        .toMap
    val refsNotInSchemaRefSchema: Map[Reference, StructType] =
      refsNotInSchema
        .map(ref =>
          ref -> StructType(Array(
            StructField(s"${ref.refName}$$${ID_COL.columnName}", IntegerType))))
        .toMap

    val projectionRefSchema: Map[Reference, StructType] =
      refsInSchemaWithIdRefSchema ++ refsInSchemaWithoutIdRefSchema ++ refsNotInSchemaRefSchema
    val projectionSchema: StructType =
      StructType(projectionRefSchema.values.flatMap(_.fields).toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = projectionRefSchema,
      sparkBtableSchema = projectionSchema,
      btableOps = SqlQuery(resQuery = sqlProject))
  }
}
