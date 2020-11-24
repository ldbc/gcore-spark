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

import algebra.expressions.AlgebraExpression

/**
  * A [[RelationalOperator]] between two [[RelationLike]]s. By default, the [[BindingSet]] of the
  * resulting [[RelationLike]] becomes the union of the [[BindingSet]]s of the two operands.
  */
abstract class BinaryOperator(lhs: RelationLike,
                              rhs: RelationLike,
                              bindingSet: Option[BindingSet] = None)
  extends RelationLike(bindingSet.getOrElse(lhs.getBindingSet ++ rhs.getBindingSet)) {

  children = List(lhs, rhs)
}

object BinaryOperator {

  /**
    * Reduces a sequence of [[BinaryOperator]]s, from left to right. The reduced result will be:
    *
    * reduce([R1, R2, R3, ... Rn], binaryOp) =
    *   binaryOp(
    *     binaryOp(R1, R2),
    *     reduce([R3, R4, ... Rn], binaryOp) = ...
    */
  def reduceLeft(relations: Seq[RelationLike],
                 binaryOp: (RelationLike, RelationLike, Option[BindingSet]) => RelationLike)
  : RelationLike = {

    relations match {
      case Seq() => RelationLike.empty
      case _ => relations.reduceLeft((agg, rel) => binaryOp(agg, rel, None))
    }
  }
}
