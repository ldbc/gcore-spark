package planner.operators

import algebra.types.Graph
import planner.target_api.TargetPlanner
import planner.trees.PlannerContext

abstract class PhysEdgeScan[QueryType](edgeScan: EdgeScan,
                                       graph: Graph,
                                       plannerContext: PlannerContext,
                                       targetPlanner: TargetPlanner)
  extends PhysEntityScan[QueryType](graph, plannerContext, targetPlanner)
