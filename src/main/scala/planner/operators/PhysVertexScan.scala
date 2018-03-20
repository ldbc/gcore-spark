package planner.operators

import algebra.types.Graph
import planner.target_api.TargetPlanner
import planner.trees.PlannerContext

abstract class PhysVertexScan[QueryOperand](vertexScan: VertexScan,
                                            graph: Graph,
                                            plannerContext: PlannerContext,
                                            targetPlanner: TargetPlanner)
  extends PhysEntityScan[QueryOperand](graph, plannerContext, targetPlanner)
