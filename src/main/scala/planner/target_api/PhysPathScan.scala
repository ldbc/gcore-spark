package planner.target_api

import planner.operators.PathScan

abstract class PhysPathScan(pathScan: PathScan)
  extends PhysEntityScan(pathScan.graph, pathScan.context) {
  children = List(pathScan)
}
