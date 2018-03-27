package planner.target_api

import planner.operators.EdgeScan

abstract class PhysEdgeScan(edgeScan: EdgeScan)
  extends PhysEntityScan(edgeScan.graph, edgeScan.context)
