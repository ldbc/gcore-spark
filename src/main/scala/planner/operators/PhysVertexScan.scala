package planner.operators

import algebra.types.Graph
import planner.target_api.TargetPlanner
import planner.trees.PlannerContext

abstract class PhysVertexScan(vertexScan: VertexScan,
                              graph: Graph,
                              plannerContext: PlannerContext,
                              targetPlanner: TargetPlanner)
  extends PhysEntityScan(graph, plannerContext, targetPlanner)
