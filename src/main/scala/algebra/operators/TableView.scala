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

package algebra.operators

import algebra.trees.ConditionalToGroupConstruct.BTABLE_VIEW

/**
  * An algebraic sub-tree with a name. Can be reused within other sub-trees, instead of the original
  * algebraic plan that it references.
  */
abstract class TableView(viewName: String, bindingSet: BindingSet)
  extends RelationLike(bindingSet) {

  override def name: String = s"${super.name} [$viewName]"

  def getViewName: String = viewName
}

/**
  * Placeholder for the binding table materialized from the MATCH clause. During the rewriting phase
  * of the algebraic tree, we don't have access to the actual matched data.
  */
case class BindingTableView(bindingSet: BindingSet) extends TableView(BTABLE_VIEW, bindingSet)

/**
  * Placeholder for the filtered binding table used in constructing the entities in a
  * [[GroupConstruct]].
  */
case class BaseConstructTableView(viewName: String, bindingSet: BindingSet)
  extends TableView(viewName, bindingSet)

/**
  * Placeholder for the table resulting after the construction of the vertices in a
  * [[GroupConstruct]].
  */
case class VertexConstructTableView(viewName: String, bindingSet: BindingSet)
  extends TableView(viewName, bindingSet)

case class ConstructRelationTableView(viewName: String, bindingSet: BindingSet)
  extends TableView(viewName, bindingSet)
