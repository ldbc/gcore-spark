package algebra.target_api

import algebra.trees.AlgebraTreeNode

/** A target-specific implementation of a node in the logical plan. */
abstract class TargetTreeNode extends AlgebraTreeNode {

  val bindingTable: BindingTableMetadata
}
