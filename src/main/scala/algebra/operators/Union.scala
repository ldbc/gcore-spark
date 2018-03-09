package algebra.operators

case class UnionAll(lhs: RelationLike,
                    rhs: RelationLike,
                    bindingContext: Option[BindingContext] = None)
  extends BinaryPrimitive(lhs, rhs, bindingContext)

case class UnionAllRelations() extends RelationLike(BindingContext.empty)
