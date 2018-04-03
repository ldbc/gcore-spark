package algebra

import algebra.trees._
import compiler.RewriteStage
import org.slf4j.{Logger, LoggerFactory}

case class AlgebraRewriter(context: AlgebraContext) extends RewriteStage {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val patternsToRelations = PatternsToRelations rewriteTree tree
    val expandedRelations = ExpandRelations(context) rewriteTree patternsToRelations
    val matchesToAlgebra = MatchesToAlgebra rewriteTree expandedRelations
    logger.info("\n{}", matchesToAlgebra.treeString())
    matchesToAlgebra
  }
}
