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

package algebra

import algebra.trees._
import compiler.{CompilationStage, RewriteStage}
import org.slf4j.{Logger, LoggerFactory}

/**
  * The aggregation of all rewriting phases that operate on the algebraic tree. By the end of this
  * [[CompilationStage]], the rewritten algebraic tree should have become a fully relational tree.
  */
case class AlgebraRewriter(context: AlgebraContext) extends RewriteStage {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    // Algebra tree analysis.
    val bindingToGraph = MapBindingToGraph(context) mapBindingToGraph tree
    val bindingContext = ExtractReferenceTuples extractReferenceTuples tree

    val enrichedContext =
      context.copy(bindingToGraph = Some(bindingToGraph), bindingContext = Some(bindingContext))

    // Match rewrite.
    val matchTree: AlgebraTreeNode = AddGraphToExistentialPatterns(enrichedContext) rewriteTree tree
    val patternsToRelations = PatternsToRelations rewriteTree matchTree
    val expandedRelations = ExpandRelations(context) rewriteTree patternsToRelations
    val matchesToAlgebra = MatchesToAlgebra rewriteTree expandedRelations

    // Construct rewrite.
    val groupConstructs = ConditionalToGroupConstruct(enrichedContext) rewriteTree matchesToAlgebra

    // Query rewrite
    val createGraph = BasicQueriesToGraphs rewriteTree groupConstructs
    logger.info("\n{}", createGraph.treeString())

    createGraph
  }
}
