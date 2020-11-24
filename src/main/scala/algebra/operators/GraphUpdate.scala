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

import algebra.expressions.{LabelRemove, PropertyRemove, PropertySet}
import common.compiler.Context

/**
  * An operation on a graph that updates its structure, such as labels or properties of its
  * entities.
  */
abstract class GraphUpdate extends GcoreOperator {

  override def checkWithContext(context: Context): Unit = {}
}

/** A wrapper over a sequence of [[PropertySet]] clauses. */
case class SetClause(propSets: Seq[PropertySet]) extends GraphUpdate {
  children = propSets
}

/** A wrapper over a sequence of [[PropertyRemove]] and [[LabelRemove]] clauses. */
case class RemoveClause(propRemoves: Seq[PropertyRemove], labelRemoves: Seq[LabelRemove])
  extends GraphUpdate {

  children = propRemoves ++ labelRemoves
}
