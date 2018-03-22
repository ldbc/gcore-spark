package planner.target_api

import algebra.types.Graph
import planner.operators.PathScan
import planner.trees.PlannerContext

abstract class PhysPathScan(pathScan: PathScan, graph: Graph, plannerContext: PlannerContext)
  extends PhysEntityScan(graph, plannerContext)
