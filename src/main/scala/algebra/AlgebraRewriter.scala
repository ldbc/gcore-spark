package algebra

import algebra.expressions.Reference
import algebra.trees._
import algebra.types.Graph
import compiler.{CompilationStage, RewriteStage}
import org.slf4j.{Logger, LoggerFactory}

/**
  * The aggregation of all rewriting phases that operate on the algebraic tree. By the end of this
  * [[CompilationStage]], the rewritten algebraic tree should have become a fully relational tree.
  */
case class AlgebraRewriter(context: AlgebraContext) extends RewriteStage {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val bindingToGraph: Map[Reference, Graph] =
      MapBindingToGraph(context) mapBindingToGraph tree
    val matchTree: AlgebraTreeNode =
      AddGraphToExistentialPatterns(context.copy(bindingToGraph = Some(bindingToGraph)))
        .rewriteTree(tree)

    val patternsToRelations = PatternsToRelations rewriteTree matchTree
    val expandedRelations = ExpandRelations(context) rewriteTree patternsToRelations

    val matchesToAlgebra = MatchesToAlgebra rewriteTree expandedRelations

    logger.info("\n{}", matchesToAlgebra.treeString())
    matchesToAlgebra
  }
}
