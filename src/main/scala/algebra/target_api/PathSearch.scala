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

package algebra.target_api

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{Relation, VirtualPathRelation}
import algebra.types.{Graph, PathExpression}
import schema.Catalog

abstract class PathSearch(pathRelation: VirtualPathRelation, graph: Graph, catalog: Catalog)
  extends EntityScan(graph, catalog) {

  val fromTableName: Label = pathRelation.fromRel.labelRelation.asInstanceOf[Relation].label
  val toTableName: Label = pathRelation.toRel.labelRelation.asInstanceOf[Relation].label

  val fromExpr: AlgebraExpression = pathRelation.fromRel.expr
  val toExpr: AlgebraExpression = pathRelation.toRel.expr

  val pathBinding: Reference = pathRelation.ref
  val fromBinding: Reference = pathRelation.fromRel.ref
  val toBinding: Reference = pathRelation.toRel.ref

  val isReachableTest: Boolean = pathRelation.isReachableTest
  val costVarDef: Option[Reference] = pathRelation.costVarDef
  val pathExpression: Option[PathExpression] = pathRelation.pathExpression

  children =
    List(pathBinding, fromBinding, toBinding, fromTableName, toTableName, fromExpr, toExpr) ++
      costVarDef.toList

  override def name: String = s"${super.name} [isReachableTest = $isReachableTest]"
}
