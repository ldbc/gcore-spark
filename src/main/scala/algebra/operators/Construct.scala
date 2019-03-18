/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
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
import algebra.types.{ConstructPattern, NamedGraph, QueryGraph}
import common.compiler.Context

/** A construct-like operator that participates in the construct sub-query of a G-CORE query. */
abstract class ConstructLike extends GcoreOperator {
  override def checkWithContext(context: Context): Unit = {}
}

/**
  * The top-most construct clause of the query that dictates how the resulting graph should be
  * built. The new graph can result from the [[GraphUnion]] of [[NamedGraph]]s or [[QueryGraph]]s,
  * unioned with graphs resulting from [[CondConstructs]]s. Additionally, the resulting graph
  * can be updated with [[SetClause]]s and [[RemoveClause]]s.
  */
case class ConstructClause(constructExp: Seq[ConstructExp]) extends ConstructLike {

  children = constructExp
}
case class ConstructExp(graphs: GraphUnion, condConstructs: CondConstructs, where: Where,
                        setClause: SetClause, removeClause: RemoveClause, having:Having) extends  ConstructLike
{
  children= List(graphs,condConstructs,where, setClause, removeClause,having)
}

/** A wrapper over a sequence of [[CondConstructClause]]s. */
case class CondConstructs(condConstructs: Seq[CondConstructClause]) extends ConstructLike {
  children = condConstructs
}

/**
  * The most basic construction clause, that specifies a [[ConstructPattern]] for the binding table.
  */
case class CondConstructClause(constructPattern: ConstructPattern)
  extends ConstructLike {

  children = List(constructPattern)
}
