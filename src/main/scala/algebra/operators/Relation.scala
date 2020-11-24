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

import algebra.expressions.Label

/**
  * A logical table in the algebraic tree. It is the binding table produced by any
  * [[RelationalOperator]]. All [[RelationLike]]s have a [[BindingSet]] that represents the header
  * of the table.
  */
abstract class RelationLike(bindingSet: BindingSet) extends RelationalOperator {

  def getBindingSet: BindingSet = bindingSet

  override def name: String = s"${super.name} [bindingSet = $bindingSet]"
}

object RelationLike {

  /** The empty binding table. */
  val empty: RelationLike = new RelationLike(BindingSet.empty) {
    override def name: String = "EmptyRelation"
  }
}

/**
  * A wrapper over a [[Label]], such that we can use the [[Label]] (which is effectively a table in
  * the database) in the relational tree.
  */
case class Relation(label: Label) extends RelationLike(BindingSet.empty) {
  children = List(label)
}

/**
  * Given that not all variables are labeled upfront in the query, they cannot be assigned a strict
  * [[Relation]] and instead can be any relation in the database.
  */
case object AllRelations extends RelationLike(BindingSet.empty)
