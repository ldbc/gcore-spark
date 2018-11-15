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

package parser.trees

import algebra.operators.{Create, Drop, View, _}
import algebra.trees.AlgebraTreeNode
import common.trees.TreeBuilder
import parser.trees.QueryTreeBuilder.extractQueryClause

/**
  * Creates the algebraic tree from the lexical tree generated with Spoofax.
  *
  * The root of the algebra tree is a [[Query]] clause, whose children are the optional path clause
  * and the mandatory [[ConstructClause]] and [[MatchClause]].
  */
object AlgebraTreeBuilder extends TreeBuilder[SpoofaxBaseTreeNode, AlgebraTreeNode] {

  override def build(from: SpoofaxBaseTreeNode): AlgebraTreeNode = {

    from.name match {
      case "BasicQuery" =>
        val query: Query = extractQueryClause(from)
        query
      case "Create" =>
        val query: Query = extractQueryClause(from.children(1))
        Create(from.children.head.asInstanceOf[SpoofaxLeaf[String]].leafValue.toString,query)
      case "Drop" =>
        Drop(from.children.head.asInstanceOf[SpoofaxLeaf[String]].leafValue.toString)
      case "View" =>
        val query: Query = extractQueryClause(from.children(1))
        View(from.children.head.asInstanceOf[SpoofaxLeaf[String]].leafValue.toString,query)


    }
  }
}
