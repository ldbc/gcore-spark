package algebra.trees

import algebra.operators._
import algebra.target_api.TargetPlanner
import common.trees.BottomUpRewriter
import schema.Catalog
import algebra.{target_api => target}

/**
  * Creates the physical plan from the logical plan. Uses a [[TargetPlanner]] to emit
  * target-specific operators. Each logical operator op of type OpType is converted into its
  * target-specific equivalent by calling the [[TargetPlanner]]'s planOpType(op) method.
  */
case class AlgebraToTargetTree(catalog: Catalog, targetPlanner: TargetPlanner)
  extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
    case SimpleMatchRelation(rel, matchContext, _) =>
      rel match {
        case vr: VertexRelation =>
          targetPlanner.planVertexScan(vr, matchContext.graph, catalog)
        case er: EdgeRelation =>
          targetPlanner.planEdgeScan(er, matchContext.graph, catalog)
        case spr: StoredPathRelation =>
          targetPlanner.planPathScan(spr, matchContext.graph, catalog)
        case vpr: VirtualPathRelation =>
          targetPlanner.planPathSearch(vpr, matchContext.graph, catalog)
      }

    case ua: UnionAll => targetPlanner.planUnionAll(ua)
    case join: JoinLike => targetPlanner.planJoin(join)
    case select: Select => targetPlanner.planSelect(select)
    case project: Project => targetPlanner.planProject(project)
    case groupBy: GroupBy => targetPlanner.planGroupBy(groupBy)
    case addColumn: AddColumn => targetPlanner.planAddColumn(addColumn)

    case tableView: TableView => targetPlanner.createTableView(tableView.getViewName)
    case ec: ConstructRelation => targetPlanner.planConstruct(ec)
    case VertexCreate(reference, removeClause) =>
      target.VertexCreate(reference, removeClause, Catalog.nextBaseEntityTableIndex)
    case EdgeCreate(reference, leftReference, rightReference, connType, removeClause) =>
      target.EdgeCreate(
        reference, leftReference, rightReference, connType,
        removeClause,
        Catalog.nextBaseEntityTableIndex)
  }
}
