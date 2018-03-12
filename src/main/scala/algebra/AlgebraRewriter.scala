package algebra

import algebra.trees.{AlgebraTreeNode, GcoreToJoinGraphRewriter}
import compiler.RewriteStage

object AlgebraRewriter extends RewriteStage {

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val newTree = GcoreToJoinGraphRewriter rewriteTree tree
    newTree printTree()
    newTree
  }
}
