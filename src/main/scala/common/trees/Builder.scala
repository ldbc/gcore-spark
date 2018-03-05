package common.trees

/** Creates a tree from any given input. */
abstract class Builder[I, O <: TreeNode[O]] {
  def build(from: I): O
}
