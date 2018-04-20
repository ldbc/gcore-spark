package planner.trees

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.exceptions.UnsupportedOperation
import common.trees.BottomUpRewriter
import planner.operators._
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
        case project: Project => targetPlanner.createPhysProject(project)
        case groupBy: GroupBy => targetPlanner.createPhysGroupBy(groupBy)
        case other =>
          throw UnsupportedOperation("Binding table operator with the following plan tree is not " +
            s"supported at the moment:\n${other.treeString()}")
      }
    case btable: BindingTable => targetPlanner.createPhysBindingTable
    case vc: VertexCreate => targetPlanner.createPhysVertexCreate(vc)
  }
}
