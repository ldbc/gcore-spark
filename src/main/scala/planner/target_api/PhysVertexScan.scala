package planner.target_api

import planner.operators.VertexScan

abstract class PhysVertexScan(vertexScan: VertexScan)
  extends PhysEntityScan(vertexScan.graph, vertexScan.context)
