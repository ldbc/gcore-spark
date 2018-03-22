package planner.target_api

import algebra.types.Graph
import planner.operators.EdgeScan
import planner.trees.PlannerContext

abstract class PhysEdgeScan(edgeScan: EdgeScan, graph: Graph, plannerContext: PlannerContext)
  extends PhysEntityScan(graph, plannerContext)
