package algebra

import algebra.trees.{AlgebraContext, AlgebraTreeNode, GcoreToJoinGraphRewriter}
import compiler.RewriteStage

case class AlgebraRewriter(context: AlgebraContext) extends RewriteStage {

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val joinGraph = GcoreToJoinGraphRewriter(context.bindingTable) rewriteTree tree
    joinGraph printTree()
    println(context.bindingTable)

    joinGraph
  }
}
