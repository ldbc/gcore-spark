package common.trees

/** Creates a tree from any given input. */
abstract class TreeBuilder[I, O <: TreeNode[O]] {
  def build(from: I): O
}
