package algebra

import algebra.trees.{AlgebraTreeNode, GcoreToAlgebraTranslation}
import compiler.RewriteStage

object AlgebraRewriter extends RewriteStage {

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val newTree = GcoreToAlgebraTranslation rewriteTree tree
    println(newTree.treeString())
    newTree
  }
}
