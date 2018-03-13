package algebra.trees

import algebra.operators._
import common.trees.TopDownRewriter

object ExpandDoubleEndpRelation extends TopDownRewriter[AlgebraTreeNode] {

  override def rule: RewriteFuncType = joinedEdgeRel

  private val joinedEdgeRel: RewriteFuncType = {
    case
      EquiJoin(
        EquiJoin(
          EdgeRelation(edgeRef, _, edgeBtable),
          VertexRelation(fromRef, _, fromBtable),
          fromLhsAttr,
          fromRhsAttr,
          _),
        to @ VertexRelation(toRef, _, toBtable),
        toLhsAttr,
        toRhsAttr,
        _) =>

      val fromRel: RelationLike = fromBtable.relations(fromRef).head
      val toRel: RelationLike = toBtable.relations(toRef).head
      val edgeRel: RelationLike = edgeBtable.relations(edgeRef).head

      val edgeRelJoinedOnFrom: RelationLike =
        SemiJoin(
          lhs = edgeRel,
          rhs = fromRel,
          lhsAttribute = fromLhsAttr,
          rhsAttribute = fromRhsAttr)

      val edgeRelJoinedOnFromAndTo: RelationLike =
        SemiJoin(
          lhs = edgeRelJoinedOnFrom,
          rhs = toRel,
          lhsAttribute = toLhsAttr,
          rhsAttribute = toRhsAttr)

      val fromRelJoinedOnEdge: RelationLike =
        SemiJoin(
          lhs = fromRel,
          rhs = edgeRelJoinedOnFromAndTo,
          lhsAttribute = fromRhsAttr,
          rhsAttribute = fromLhsAttr)

      val toRelJoinedOnEdge: RelationLike =
        SemiJoin(
          lhs = toRel,
          rhs = edgeRelJoinedOnFromAndTo,
          lhsAttribute = toRhsAttr,
          rhsAttribute = toLhsAttr)

      EdgeRelation(
        ref = edgeRef,
        relation = RelationLike.empty,
        bindingTable =
          new BindingTable(Map(
            fromRef -> fromRelJoinedOnEdge,
            toRef -> toRelJoinedOnEdge,
            edgeRef -> edgeRelJoinedOnFromAndTo)))
  }
}
