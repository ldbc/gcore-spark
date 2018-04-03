package planner.trees

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.trees.BottomUpRewriter
import planner.operators.{BindingTableOp, EdgeScan, PathScan, VertexScan}

case class AlgebraToPlannerTree(context: PlannerContext) extends BottomUpRewriter[AlgebraTreeNode] {

  private val query: RewriteFuncType = {
    case q: Query => q.children.head
  }

  private val simpleMatchRelation: RewriteFuncType = {
    case SimpleMatchRelation(rel, matchContext, _) =>
      rel match {
        case vr: VertexRelation => VertexScan(vr, matchContext.graph, context)
        case er: EdgeRelation => EdgeScan(er, matchContext.graph, context)
        case pr: StoredPathRelation => PathScan(pr, matchContext.graph, context)
      }
  }

  private val bindingTableOp: RewriteFuncType = {
    case op: UnionAll => BindingTableOp(op)
    case op: InnerJoin => BindingTableOp(op)
    case op: LeftOuterJoin => BindingTableOp(op)
    case op: CrossJoin => BindingTableOp(op)
    case op: Select => BindingTableOp(op)
  }

  override val rule: RewriteFuncType = query orElse simpleMatchRelation orElse bindingTableOp
}
