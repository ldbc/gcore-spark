package planner.target_api

import algebra.trees.AlgebraTreeNode
import common.trees.BottomUpRewriter
import planner.operators._

abstract class TargetPlanner extends BottomUpRewriter[AlgebraTreeNode] {

  type QueryOperand
  type StorageType

  def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan[QueryOperand]

  def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan[QueryOperand]

  override val rule: RewriteFuncType = {
    case vs @ VertexScan(_, _, _) => createPhysVertexScan(vs)
    case es @ EdgeScan(_, _, _) => createPhysEdgeScan(es)
  }
}
