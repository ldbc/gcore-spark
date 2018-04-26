package algebra.target_api

import algebra.expressions.Reference

abstract class VertexCreate(reference: Reference,
                            relation: TargetTreeNode) extends TargetTreeNode {
  children = List(reference, relation)
}
