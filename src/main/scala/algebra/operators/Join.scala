package algebra.operators

import algebra.expressions.Reference

abstract class JoinLike(lhs: RelationLike,
                        rhs: RelationLike,
                        bindingTable: Option[BindingTable])
  extends BinaryPrimitive(lhs, rhs, bindingTable) {

  /**
    * Returns all the bindings that appear in at least two binding sets that have been seen so far
    * by this [[JoinLike]].
    */
  def commonInSeenBindingSets: Set[Reference] = BindingTable.intersectBindingSets(seenBindingSets)

  /** The sets of bindings that have been seen so far by in this [[JoinLike subtree. */
  val seenBindingSets: Seq[Set[Reference]] = {
    var union: Seq[Set[Reference]] = Seq.empty

    lhs match {
      case joinLike: JoinLike => union = union ++ joinLike.seenBindingSets
      case _ => union = union ++ Set(lhs.getBindingTable.bindingSet)
    }

    rhs match {
      case joinLike: JoinLike => union = union ++ joinLike.seenBindingSets
      case _ => union = union ++ Set(rhs.getBindingTable.bindingSet)
    }

    union
  }
}

case class InnerJoin(lhs: RelationLike,
                     rhs: RelationLike,
                     bindingTable: Option[BindingTable] = None)
  extends JoinLike(lhs, rhs, bindingTable)

case class EquiJoin(lhs: RelationLike,
                    rhs: RelationLike,
                    lhsAttribute: Attribute,
                    rhsAttribute: Attribute,
                    bindingTable: Option[BindingTable] = None)
  extends JoinLike(lhs, rhs, bindingTable) {

  children = List(lhs, rhs, lhsAttribute, rhsAttribute)
}

case class SemiJoin(lhs: RelationLike,
                    rhs: RelationLike,
                    lhsAttribute: Attribute,
                    rhsAttribute: Attribute,
                    bindingTable: Option[BindingTable] = None)
  extends JoinLike(lhs, rhs, bindingTable) {

  children = List(lhs, rhs, lhsAttribute, rhsAttribute)
}

case class LeftOuterJoin(lhs: RelationLike,
                         rhs: RelationLike,
                         bindingTable: Option[BindingTable] = None)
  extends JoinLike(lhs, rhs, bindingTable)

case class FullOuterJoin(lhs: RelationLike,
                         rhs: RelationLike,
                         bindingTable: Option[BindingTable] = None)
  extends JoinLike(lhs, rhs, bindingTable)
