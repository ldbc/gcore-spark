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

package algebra.trees

import algebra.expressions.ConjunctLabels
import common.compiler.Context
import schema.{EntitySchema, Catalog, GraphSchema}

/** A [[SemanticCheck]]able node implements validation rules that are checked upon creation. */
trait SemanticCheck {

  check()

  def check(): Unit
}

/**
  * A node that extends [[SemanticCheckWithContext]] implements validation rules that require an
  * outer [[Context]]. Hence, the validation cannot be performed by the node itself and needs to be
  * triggered by the provider of the [[Context]].
  */
trait SemanticCheckWithContext {

  def checkWithContext(context: Context)
}

/** Available [[Context]]s for semantic checks. */
case class QueryContext(catalog: Catalog) extends Context
case class GraphPatternContext(schema: GraphSchema, graphName: String) extends Context
case class DisjunctLabelsContext(graphName: String, schema: EntitySchema) extends Context
case class PropertyContext(graphName: String,
                           labelsExpr: Option[ConjunctLabels],
                           schema: EntitySchema) extends Context
