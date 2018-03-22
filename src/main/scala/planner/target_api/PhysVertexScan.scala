package planner.target_api

import algebra.types.Graph
import planner.operators.VertexScan
import planner.trees.PlannerContext

abstract class PhysVertexScan(vertexScan: VertexScan, graph: Graph, plannerContext: PlannerContext)
  extends PhysEntityScan(graph, plannerContext)
