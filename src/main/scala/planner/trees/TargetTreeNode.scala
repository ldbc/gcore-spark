package planner.trees

import planner.operators.BindingTable

abstract class TargetTreeNode extends PlannerTreeNode {

  val bindingTable: BindingTable
}
