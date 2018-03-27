package planner.target_api

import algebra.operators.{JoinLike, UnionAll}
import planner.operators.{EdgeScan, PathScan, VertexScan}
import planner.trees.PlannerTreeNode

abstract class TargetPlanner {

  type StorageType

  def createBindingTable(input: PlannerTreeNode): StorageType

  def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan

  def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan

  def createPhysPathScan(pathScanOp: PathScan): PhysPathScan

  def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll

  def createPhysJoin(joinOp: JoinLike): PhysJoin
}
