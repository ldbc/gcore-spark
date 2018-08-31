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

package algebra.expressions

abstract class AggregateExpression(distinct: Boolean, expr: AlgebraExpression, symbol: String)
  extends UnaryExpression(expr, symbol) {

  override def name: String = s"${super.name} [distinct = $distinct]"

  def isDistinct: Boolean = distinct
}

case class Collect(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "COLLECT_LIST")

case class Count(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "COUNT")

case class Min(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "MIN")

case class Max(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "MAX")

case class Sum(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "SUM")

case class Avg(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "AVG")

case class GroupConcat(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "GROUP_CONCAT")
