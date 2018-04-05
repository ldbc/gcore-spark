package planner.target_api

import algebra.operators.{JoinLike, Select, UnionAll}
import planner.operators.{EdgeScan, PathScan, VertexScan}
import planner.trees.{PlannerTreeNode, TargetTreeNode}

/**
  * A target-specific implementation should provide concrete [[TargetTreeNode]]s in order to create
  * the physical plan.
  */
abstract class TargetPlanner {

  /** The target-specific storage type for the tables holding vertex, edge and path data. */
  type StorageType

  /** Creates the binding table from the match clause. */
  def createBindingTable(input: PlannerTreeNode): StorageType

  def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan

  def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan

  def createPhysPathScan(pathScanOp: PathScan): PhysPathScan

  def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll

  def createPhysJoin(joinOp: JoinLike): PhysJoin

  def createPhysSelect(selectOp: Select): PhysSelect
}
