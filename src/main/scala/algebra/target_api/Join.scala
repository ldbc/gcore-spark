package algebra.target_api

abstract class Join(lhs: TargetTreeNode, rhs: TargetTreeNode) extends TargetTreeNode {

  children = List(lhs, rhs)
}
