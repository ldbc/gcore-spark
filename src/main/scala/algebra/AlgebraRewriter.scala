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
    val groupingSets = CreateGroupingSets(enrichedContext) rewriteTree matchesToAlgebra
    logger.info("\n{}", groupingSets.treeString())

    // Query rewrite
    val createGraph = BasicQueriesToGraphs rewriteTree groupingSets
    logger.info("\n{}", createGraph.treeString())

    createGraph
  }
}
