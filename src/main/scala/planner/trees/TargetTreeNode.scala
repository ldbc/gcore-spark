package planner.trees

import planner.target_api.BindingTable

/** A target-specific implementation of a node in the logical plan. */
abstract class TargetTreeNode extends PlannerTreeNode {

  val bindingTable: BindingTable
}
