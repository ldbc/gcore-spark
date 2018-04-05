package planner.trees

import algebra.operators.{JoinLike, Select, UnionAll}
import algebra.trees.AlgebraTreeNode
import common.exceptions.UnsupportedOperation
import common.trees.BottomUpRewriter
import planner.operators.{BindingTableOp, EdgeScan, PathScan, VertexScan}
import planner.target_api.TargetPlanner

/**
  * Creates the physical plan from the logical plan. Uses a [[TargetPlanner]] to emit
  * target-specific operators. Each logical operator lop of type LopType is converted into its
  * target-specific equivalent by calling the [[TargetPlanner]]'s createPhysLopType(lop) method.
  */
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
        case select: Select => targetPlanner.createPhysSelect(select)
        case other =>
          throw UnsupportedOperation("Match pattern resulting in the following plan tree is not " +
            s"supported at the moment:\n${other.treeString()}")
      }
  }
}
