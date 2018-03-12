package algebra.operators

import algebra.expressions.Reference

abstract class JoinLike(lhs: RelationLike,
                        rhs: RelationLike,
                        bindingContext: Option[BindingContext])
  extends BinaryPrimitive(lhs, rhs, bindingContext) {

  /**
    * Returns all the bindings that appear in at least two binding sets that have been seen so far
    * by this [[JoinLike]].
    */
  def commonInSeenBindingSets: BindingSet = BindingSet.intersect(seenBindingSets)

  /** The set of [[BindingSet]]s that have been seen so far by in this [[JoinLike]] subtree. */
  val seenBindingSets: Seq[BindingSet] = {
    var union: Seq[BindingSet] = Seq.empty

    lhs match {
      case joinLike: JoinLike => union = union ++ joinLike.seenBindingSets
      case _ => union = union ++ Set(lhs.getBindings)
    }

    rhs match {
      case joinLike: JoinLike => union = union ++ joinLike.seenBindingSets
      case _ => union = union ++ Set(rhs.getBindings)
    }

    union
  }
}

case class InnerJoin(lhs: RelationLike,
                     rhs: RelationLike,
                     bindingContext: Option[BindingContext] = None)
  extends JoinLike(lhs, rhs, bindingContext)

case class EquiJoin(lhs: RelationLike,
                    rhs: RelationLike,
                    lhsAttribute: Reference,
                    rhsAttribute: Reference,
                    bindingContext: Option[BindingContext] = None)
  extends JoinLike(lhs, rhs, bindingContext)

case class LeftOuterJoin(lhs: RelationLike,
                         rhs: RelationLike,
                         bindingContext: Option[BindingContext] = None)
  extends JoinLike(lhs, rhs, bindingContext)
