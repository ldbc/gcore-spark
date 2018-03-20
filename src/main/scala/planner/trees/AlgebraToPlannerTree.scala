package planner.trees

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.trees.BottomUpRewriter
import planner.operators.{EdgeScan, VertexScan}

case class AlgebraToPlannerTree(context: PlannerContext)
  extends BottomUpRewriter[AlgebraTreeNode] {

  private val query: RewriteFuncType = {
    case q @ Query(_) => q.children.head
  }

  private val condMatchRelation: RewriteFuncType = {
    case m @ CondMatchRelation(_, _, _) => m.children.head
  }

  private val simpleMatchRelation: RewriteFuncType = {
    case SimpleMatchRelation(relation @ VertexRelation(_, _, _), matchContext, _) =>
      VertexScan(relation, matchContext.graph, context)

    case SimpleMatchRelation(relation @ EdgeRelation(_, _, _, _, _), matchContext, _) =>
      EdgeScan(relation, matchContext.graph, context)
  }

  override val rule: RewriteFuncType = query orElse condMatchRelation orElse simpleMatchRelation
}
