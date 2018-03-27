package planner.trees

import algebra.operators.{JoinLike, UnionAll}
import algebra.trees.AlgebraTreeNode
import common.trees.BottomUpRewriter
import planner.exceptions.UnsupportedOperation
import planner.operators.{BindingTableOp, EdgeScan, PathScan, VertexScan}
import planner.target_api.TargetPlanner

case class PlannerToTargetTree(targetPlanner: TargetPlanner)
  extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
    case vs: VertexScan => targetPlanner.createPhysVertexScan(vs)
    case es: EdgeScan => targetPlanner.createPhysEdgeScan(es)
    case ps: PathScan => targetPlanner.createPhysPathScan(ps)
    case BindingTableOp(op) =>
      op match {
        case ua: UnionAll => targetPlanner.createPhysUnionAll(ua)
        case join: JoinLike => targetPlanner.createPhysJoin(join)
        case other =>
          throw UnsupportedOperation("Match pattern resulting in the following plan tree is not " +
            s"supported at the moment:\n${other.treeString()}")
      }
  }

}
