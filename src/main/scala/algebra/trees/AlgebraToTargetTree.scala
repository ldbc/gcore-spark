package algebra.trees

import algebra.operators._
import algebra.target_api.TargetPlanner
import common.trees.BottomUpRewriter
import schema.GraphDb

/**
  * Creates the physical plan from the logical plan. Uses a [[TargetPlanner]] to emit
  * target-specific operators. Each logical operator op of type OpType is converted into its
  * target-specific equivalent by calling the [[TargetPlanner]]'s planOpType(op) method.
  */
case class AlgebraToTargetTree(graphDb: GraphDb, targetPlanner: TargetPlanner)
  extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
    case SimpleMatchRelation(rel, matchContext, _) =>
      rel match {
        case vr: VertexRelation =>
          targetPlanner.planVertexScan(vr, matchContext.graph, graphDb)
        case er: EdgeRelation =>
          targetPlanner.planEdgeScan(er, matchContext.graph, graphDb)
        case pr: StoredPathRelation =>
          targetPlanner.planPathScan(pr, matchContext.graph, graphDb)
      }

    case ua: UnionAll => targetPlanner.planUnionAll(ua)
    case join: JoinLike => targetPlanner.planJoin(join)
    case select: Select => targetPlanner.planSelect(select)
    case project: Project => targetPlanner.planProject(project)
    case groupBy: GroupBy => targetPlanner.planGroupBy(groupBy)
    case addColumn: AddColumn => targetPlanner.planAddColumn(addColumn)

    case tableView: TableView => targetPlanner.createTableView(tableView.getViewName)
    case ec: ConstructRelation => targetPlanner.planConstruct(ec)
  }
}
