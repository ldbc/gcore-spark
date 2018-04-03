package planner.target_api

import algebra.expressions.AlgebraExpression
import planner.trees.TargetTreeNode

abstract class PhysSelect(relation: TargetTreeNode, expr: AlgebraExpression)
  extends TargetTreeNode {

  children = List(relation, expr)
}
