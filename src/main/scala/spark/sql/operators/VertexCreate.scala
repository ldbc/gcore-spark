package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}

case class VertexCreate(reference: Reference, relation: TargetTreeNode)
  extends target_api.VertexCreate(reference, relation) {

  override val bindingTable: BindingTableMetadata = relation.bindingTable
}
