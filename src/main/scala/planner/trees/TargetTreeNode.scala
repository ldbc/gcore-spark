package planner.trees

import planner.operators.BindingTable
import planner.target_api.TargetPlanner

abstract class TargetTreeNode(targetPlanner: TargetPlanner) extends PlannerTreeNode {

  val bindingTable: BindingTable
}
