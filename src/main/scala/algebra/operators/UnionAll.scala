package algebra.operators

/**
  * The relational union of two [[RelationLike]]s. The two unioned tables should have the same
  * header. Thus, the [[BindingSet]] of this operation remains the [[BindingSet]] of either one of
  * the operations.
  */
case class UnionAll(lhs: RelationLike,
                    rhs: RelationLike,
                    bindingTable: Option[BindingSet] = None)
  extends BinaryOperator(lhs, rhs, bindingTable)
