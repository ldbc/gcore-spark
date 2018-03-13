package algebra

import algebra.trees.{AlgebraContext, AlgebraTreeNode, ExpandDoubleEndpRelation, GcoreToJoinGraph}
import compiler.RewriteStage

case class AlgebraRewriter(context: AlgebraContext) extends RewriteStage {

  override def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    val joinGraph = GcoreToJoinGraph rewriteTree tree
    joinGraph printTree()

    val expandedGraph = ExpandDoubleEndpRelation rewriteTree joinGraph
    expandedGraph printTree()

    expandedGraph
  }
}
