package algebra.operators

case class CartesianProduct(lhs: RelationLike,
                            rhs: RelationLike,
                            bindingContext: Option[BindingContext] = None)
  extends BinaryPrimitive(lhs, rhs, bindingContext)
