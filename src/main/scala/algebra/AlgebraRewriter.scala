package algebra

import algebra.trees._
import compiler.RewriteStage

case class AlgebraRewriter(context: AlgebraContext) extends RewriteStage {

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val patternsToRelations = PatternsToRelations rewriteTree tree
    val expandedRelations = ExpandRelations(context) rewriteTree patternsToRelations
    val patternsToAlgebra = PatternsToAlgebra rewriteTree expandedRelations
    val matchesToAlgebra = MatchesToAlgebra rewriteTree patternsToAlgebra
    matchesToAlgebra printTree()
    matchesToAlgebra
  }
}
