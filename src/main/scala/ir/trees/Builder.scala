package ir.trees

abstract class Builder[I, O <: TreeNode[O]] {
  def build(from: I): O
}
