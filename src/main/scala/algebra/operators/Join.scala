package algebra.operators

import algebra.expressions.Reference

abstract class JoinLike(lhs: RelationLike,
                        rhs: RelationLike,
                        bindingTable: Option[BindingSet])
  extends BinaryPrimitive(lhs, rhs, bindingTable) {

  /**
    * Returns all the bindings that appear in at least two binding sets that have been seen so far
    * by this [[JoinLike]].
    */
  def commonInSeenBindingSets: Set[Reference] = BindingSet.intersectBindingTables(seenBindingSets)

  /** The sets of bindings that have been seen so far by in this [[JoinLike]] subtree. */
  val seenBindingSets: Seq[Set[Reference]] = {
    var union: Seq[Set[Reference]] = Seq.empty

    lhs match {
      case joinLike: JoinLike => union = union ++ joinLike.seenBindingSets
      case _ => union = union ++ Set(lhs.getBindingTable.refSet)
    }

    rhs match {
      case joinLike: JoinLike => union = union ++ joinLike.seenBindingSets
      case _ => union = union ++ Set(rhs.getBindingTable.refSet)
    }

    union
  }
}

case class LeftOuterJoin(lhs: RelationLike,
                         rhs: RelationLike,
                         bindingTable: Option[BindingSet] = None)
  extends JoinLike(lhs, rhs, bindingTable)

case class InnerJoin(lhs: RelationLike,
                     rhs: RelationLike,
                     bindingTable: Option[BindingSet] = None)
  extends JoinLike(lhs, rhs, bindingTable)

case class CrossJoin(lhs: RelationLike,
                     rhs: RelationLike,
                     bindingTable: Option[BindingSet] = None)
  extends JoinLike(lhs, rhs, bindingTable)
