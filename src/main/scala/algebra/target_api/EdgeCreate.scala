package algebra.target_api

import algebra.expressions.Reference
import algebra.types.ConnectionType

abstract class EdgeCreate(reference: Reference,
                          leftReference: Reference,
                          rightReference: Reference,
                          connType: ConnectionType,
                          relation: TargetTreeNode) extends TargetTreeNode {
  children = List(reference, leftReference, rightReference, connType, relation)
}
