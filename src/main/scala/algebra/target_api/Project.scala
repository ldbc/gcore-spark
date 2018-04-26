package algebra.target_api

import algebra.expressions.Reference

abstract class Project(relation: TargetTreeNode, attributes: Seq[Reference])
  extends TargetTreeNode {

  children = relation +: attributes
}
