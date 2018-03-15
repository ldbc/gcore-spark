package algebra.trees

import algebra.expressions.Reference
import algebra.operators._
import common.trees.TopDownRewriter

import scala.collection.mutable

object PatternsToAlgebra extends TopDownRewriter[AlgebraTreeNode] {
  val idAttr: Attribute = Attribute(Reference("id"))
  val fromIdAttr: Attribute = Attribute(Reference("fromId"))
  val toIdAttr: Attribute = Attribute(Reference("toId"))

  override def rule: RewriteFuncType = {
    case SimpleMatchRelation(v @ VertexRelation(ref, labelRelation, expr), matchContext, _) =>
      val vertexRelBtable: BindingTable = v.getBindingTable
      val select: RelationLike = Select(labelRelation, expr)
      vertexRelBtable.btable.update(ref, mutable.Set(select))
      SimpleMatchRelation(v, matchContext)

    case SimpleMatchRelation(
      e @ EdgeRelation(edgeRef, edgeLabelRel, edgeExpr, fromRel, toRel), matchContext, _) =>
      val fromSelect: RelationLike = Select(fromRel.labelRelation, fromRel.expr)
      val toSelect: RelationLike = Select(toRel.labelRelation, toRel.expr)
      val edgeRelJoinedOnFrom: RelationLike =
        EquiJoin(
          lhs = edgeLabelRel,
          rhs = fromSelect,
          lhsAttribute = idAttr,
          rhsAttribute = fromIdAttr)
      val edgeRelJoinedOnFromAndTo: RelationLike =
        EquiJoin(
          lhs = edgeRelJoinedOnFrom,
          rhs = toSelect,
          lhsAttribute = idAttr,
          rhsAttribute = toIdAttr)
      val fromJoinedOnEdge: RelationLike =
        EquiJoin(
          lhs = fromSelect,
          rhs = edgeRelJoinedOnFromAndTo,
          lhsAttribute = fromIdAttr,
          rhsAttribute = idAttr)
      val toJoinedOnEdge: RelationLike =
        EquiJoin(
          lhs = toSelect,
          rhs = edgeRelJoinedOnFromAndTo,
          lhsAttribute = toIdAttr,
          rhsAttribute = idAttr)
      val edgeBtable: BindingTable = e.getBindingTable
      val fromBtable: BindingTable = fromRel.getBindingTable
      val toBtable: BindingTable = toRel.getBindingTable
      fromBtable.btable.update(fromRel.ref, mutable.Set(fromJoinedOnEdge))
      toBtable.btable.update(toRel.ref, mutable.Set(toJoinedOnEdge))
      edgeBtable.btable.update(edgeRef, mutable.Set(edgeRelJoinedOnFromAndTo))
      edgeBtable.btable.update(fromRel.ref, mutable.Set(fromJoinedOnEdge))
      edgeBtable.btable.update(toRel.ref, mutable.Set(toJoinedOnEdge))
      SimpleMatchRelation(e, matchContext)
  }
}
