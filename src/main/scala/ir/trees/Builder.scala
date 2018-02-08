package ir.trees

/** Creates a tree from any given of input. */
abstract class Builder[I, O <: TreeNode[O]] {
  def build(from: I): O
}
