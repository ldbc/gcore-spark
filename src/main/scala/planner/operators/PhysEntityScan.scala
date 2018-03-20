package planner.operators

import algebra.types.Graph
import planner.target_api.TargetPlanner
import planner.trees.{PlannerContext, TargetTreeNode}

abstract class PhysEntityScan[QueryOperand](graph: Graph,
                                            plannerContext: PlannerContext,
                                            targetPlanner: TargetPlanner)
  extends TargetTreeNode(graph, plannerContext)
