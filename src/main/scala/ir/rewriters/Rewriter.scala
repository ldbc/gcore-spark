package ir.rewriters

import ir.trees.TreeNode

abstract class Rewriter[T <: TreeNode[T]] {
  def rule: PartialFunction[T, T]

  def rewriteDown(tree: T): T = {
    tree.transformDown(rule)
  }
}
