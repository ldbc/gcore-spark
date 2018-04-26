package algebra.target_api

import algebra.expressions.AlgebraExpression

abstract class Select(relation: TargetTreeNode, expr: AlgebraExpression)
  extends TargetTreeNode {

  children = List(relation, expr)
}
