package planner.trees

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.exceptions.UnsupportedOperation
import common.trees.BottomUpRewriter
import planner.operators._

/**
  * Creates the logical plan from the algebraic tree. Entity relation is transformed into an
  * [[EntityScan]], while the other relational operators are wrapped into a
  * [[BindingTableOp]]erator.
  */
case class AlgebraToPlannerTree(context: PlannerContext) extends BottomUpRewriter[AlgebraTreeNode] {

  private val query: RewriteFuncType = {
    case q: Query =>
      val graphUnion = q.getConstructClause.children.head.asInstanceOf[GraphUnion]
      if (graphUnion.graphs.nonEmpty)
        throw UnsupportedOperation("Graph union is not supported in CONSTRUCT clause.")

      val condConstruct = q.getConstructClause.children(1)
      CreateGraph(
        matchClause = q.getMatchClause.asInstanceOf[PlannerTreeNode],
        constructClauses = condConstruct.children.map(_.asInstanceOf[PlannerTreeNode]))
  }

  private val simpleMatchRelation: RewriteFuncType = {
    case SimpleMatchRelation(rel, matchContext, _) =>
      rel match {
        case vr: VertexRelation => VertexScan(vr, matchContext.graph, context)
        case er: EdgeRelation => EdgeScan(er, matchContext.graph, context)
        case pr: StoredPathRelation => PathScan(pr, matchContext.graph, context)
      }
  }

  private val vertexConstructRelation: RewriteFuncType = {
    case vertex: VertexConstructRelation =>
      VertexCreate(
        vertex.reference,
        vertex.children(1).asInstanceOf[PlannerTreeNode],
        vertex.expr,
        vertex.setClause,
        vertex.removeClause)
  }

  private val bindingTableOp: RewriteFuncType = {
    case op: UnionAll => BindingTableOp(op)
    case op: InnerJoin => BindingTableOp(op)
    case op: LeftOuterJoin => BindingTableOp(op)
    case op: CrossJoin => BindingTableOp(op)
    case op: Select => BindingTableOp(op)
    case op: GroupBy => BindingTableOp(op)
    case op: Project => BindingTableOp(op)
  }

  override val rule: RewriteFuncType =
    query orElse simpleMatchRelation orElse bindingTableOp orElse vertexConstructRelation
}
