package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.TargetTreeNode
import algebra.types.ConnectionType

case class EdgeCreate(reference: Reference,
                      leftReference: Reference,
                      rightReference: Reference,
                      connType: ConnectionType,
                      relation: TargetTreeNode)
  extends target_api.EdgeCreate(reference, leftReference, rightReference, connType, relation) {

  override val bindingTable: target_api.BindingTableMetadata = relation.bindingTable
}
