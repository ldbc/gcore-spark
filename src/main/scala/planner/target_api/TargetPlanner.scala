package planner.target_api

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.trees.BottomUpRewriter
import planner.exceptions.UnsupportedOperation
import planner.operators._

abstract class TargetPlanner extends BottomUpRewriter[AlgebraTreeNode] {

  type StorageType

  def createPhysVertexScan(vertexScanOp: VertexScan): PhysVertexScan

  def createPhysEdgeScan(edgeScanOp: EdgeScan): PhysEdgeScan

  def createPhysUnionAll(unionAllOp: UnionAll): PhysUnionAll

  def createPhysJoin(joinOp: JoinLike): PhysJoin

  override val rule: RewriteFuncType = {
    case vs @ VertexScan(_, _, _) => createPhysVertexScan(vs)
    case es @ EdgeScan(_, _, _) => createPhysEdgeScan(es)
    case BindingTableOp(op @ UnionAll(_, _, _)) => createPhysUnionAll(op)
    case BindingTableOp(op @ InnerJoin(_, _, _)) => createPhysJoin(op)
    case BindingTableOp(op @ CrossJoin(_, _, _)) => createPhysJoin(op)
    case BindingTableOp(op @ LeftOuterJoin(_, _, _)) => createPhysJoin(op)
    case BindingTableOp(other) =>
      throw UnsupportedOperation("Match pattern resulting in the following plan tree is not " +
        s"supported at the moment:\n${other.treeString()}")
  }
}
