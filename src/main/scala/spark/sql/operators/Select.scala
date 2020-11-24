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

import algebra.expressions.AlgebraExpression
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import spark.sql.SqlQuery
import spark.sql.SqlQuery._

/**
  * Projects all the columns from the [[relation]] filtered by the algebraic [[expr]]ession. The
  * [[relation]] is aliased before the expansion [[expr]]ession into a selection predicate, in case
  * the [[expr]]ession contains an EXISTS clause. For more details, see
  * [[expressionToSelectionPred]].
  */
case class Select(relation: TargetTreeNode, expr: AlgebraExpression)
  extends target_api.Select(relation, expr) {

  override val bindingTable: BindingTableMetadata = {
    val relationBtable: SqlBindingTableMetadata =
      relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]

    val fromAlias: String = tempViewAlias

    val selectQuery: String =
      s"""
      SELECT * FROM (${relationBtable.btable.resQuery}) $fromAlias
      WHERE (${expressionToSelectionPred(expr, relationBtable.schemaMap, fromAlias)})"""

    relationBtable.copy(btableOps = SqlQuery(resQuery = selectQuery))
  }
}
