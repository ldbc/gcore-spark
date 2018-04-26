package algebra.target_api

abstract class UnionAll(lhs: TargetTreeNode, rhs: TargetTreeNode) extends TargetTreeNode {

  children = List(lhs, rhs)
}
