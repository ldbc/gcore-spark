package api.trees

/** Applies a transformation over a tree, potentially modifying the initial structure. */
abstract class Rewriter[T <: TreeNode[T]] {

  type RewriteFuncType = PartialFunction[T, T]

  /**
    * A [[PartialFunction]] that dictates how a node in the input tree should be transformed during
    * the rewrite process.
    */
  def rule: RewriteFuncType

  def rewriteTree(tree: T): T
}

/** Transforms a tree from root to leaves. */
abstract class TopDownRewriter[T <: TreeNode[T]] extends Rewriter[T] {

  override def rewriteTree(tree: T): T = {
    tree.transformDown(rule)
  }
}

/** Transforms a tree from leaves to root. */
abstract class BottomUpRewriter[T <: TreeNode[T]] extends Rewriter[T] {

  override def rewriteTree(tree: T): T = {
    tree.transformUp(rule)
  }
}
