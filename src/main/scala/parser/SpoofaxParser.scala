/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
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

package parser

import compiler.ParseStage
import algebra.operators.{Create, Drop, Query}
import algebra.trees.{AlgebraTreeNode, QueryContext}
import org.metaborg.spoofax.core.Spoofax
import org.slf4j.{Logger, LoggerFactory}
import org.spoofax.interpreter.terms.IStrategoTerm
import parser.trees._
import parser.utils.GcoreLang

/**
  * A [[ParseStage]] that uses [[Spoofax]] to parse an incoming query. The sub-stages of this
  * parser are:
  *
  * <ul>
  *   <li> Generate the tree of [[IStrategoTerm]]s created by [[Spoofax]] </li>
  *   <li> Rewrite this tree to name all unbounded variables. </li>
  *   <li> Build the algebraic tree from the canonicalized parse tree. </li>
  * </ul>
  */
case class SpoofaxParser(context: ParseContext) extends ParseStage {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def parse(query: String): AlgebraTreeNode = {
    val ast: IStrategoTerm = GcoreLang.parseQuery(query)
    val parseTree: SpoofaxBaseTreeNode = SpoofaxTreeBuilder build ast
    val rewriteParseTree: SpoofaxBaseTreeNode = SpoofaxCanonicalRewriter rewriteTree parseTree
    val algebraTree: AlgebraTreeNode = AlgebraTreeBuilder build rewriteParseTree
    logger.info("\n{}", algebraTree.treeString())
    algebraTree match {
      case query: Query =>
        query.checkWithContext(QueryContext(context.catalog))
        query
      case create: Create =>
        algebraTree.asInstanceOf[Create].query.checkWithContext(QueryContext(context.catalog))
        create.exist = context.catalog.hasGraph(create.graphName)
        create
      case drop: Drop =>
        drop.exist = context.catalog.hasGraph(drop.graphName)
        drop
    }
  }
}
