package ir.rewriters

import ir.trees.TreeNode

/** Applies a transformation over a tree, potentially modifying the initial structure. */
abstract class Rewriter[T <: TreeNode[T]] {

  /**
    * A [[PartialFunction]] that dictates how a node in the input tree should be transformed during
    * the rewrite process.
    */
  def rule: PartialFunction[T, T]

  /** Rewrites a tree from top (root) to bottom (leaves). */
  def rewriteDown(tree: T): T = {
    tree.transformDown(rule)
  }
}
