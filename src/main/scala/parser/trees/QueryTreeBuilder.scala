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

package parser.trees

import algebra.operators._
import parser.exceptions.QueryParseException
import parser.trees.ConstructTreeBuilder._
import parser.trees.MatchTreeBuilder._

object QueryTreeBuilder {

  def extractQueryClause(from: SpoofaxBaseTreeNode): Query = {
    from.name match {
      case "BasicQuery" =>
        //        val pathClause = extractClause(from.children.head)
        val constructClause = extractClause(from.children(1)).asInstanceOf[ConstructClause]
        val matchClause = extractClause(from.children(2)).asInstanceOf[MatchClause]

        Query(constructClause, matchClause)

      case _ => throw QueryParseException(s"Query type ${from.name} unsupported for the moment.")
    }
  }

  /**
    * Builds the subtree of one of the possible clauses in a [[Query]]: path, [[ConstructClause]]
    * and [[MatchClause]].
    */
  private def extractClause(from: SpoofaxBaseTreeNode): AlgebraOperator = {
    from.name match {
      case "Construct" => extractConstructClause(from)
      case "Match" => extractMatchClause(from)
      case _ => throw QueryParseException(s"Cannot extract clause from node type ${from.name}")
    }
  }
}
