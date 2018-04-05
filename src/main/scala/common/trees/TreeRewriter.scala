package common.trees

/** Applies a transformation over a tree, creating a new tree of the same type. */
abstract class TreeRewriter[T <: TreeNode[T]] {

  type RewriteFuncType = PartialFunction[T, T]

  /**
    * A [[PartialFunction]] that dictates how a node in the input tree should be transformed during
    * the rewrite process.
    */
  val rule: RewriteFuncType

  def rewriteTree(tree: T): T
}

/** Transforms a tree from root to leaves. */
abstract class TopDownRewriter[T <: TreeNode[T]] extends TreeRewriter[T] {

  override def rewriteTree(tree: T): T = {
    tree.transformDown(rule)
  }
}

/** Transforms a tree from leaves to root. */
abstract class BottomUpRewriter[T <: TreeNode[T]] extends TreeRewriter[T] {

  override def rewriteTree(tree: T): T = {
    tree.transformUp(rule)
  }
}
