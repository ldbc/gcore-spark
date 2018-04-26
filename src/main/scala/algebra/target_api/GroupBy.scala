package algebra.target_api

import algebra.expressions.{AlgebraExpression, PropertySet}
import algebra.trees.AlgebraTreeNode

abstract class GroupBy(relation: TargetTreeNode,
                       groupingAttributes: Seq[AlgebraTreeNode],
                       aggregateFunctions: Seq[PropertySet],
                       having: Option[AlgebraExpression])
  extends TargetTreeNode  {

  children = (relation +: groupingAttributes) ++ aggregateFunctions ++ having.toList
}
