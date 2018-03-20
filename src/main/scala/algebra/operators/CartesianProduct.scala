package algebra.operators

case class CartesianProduct(lhs: RelationLike,
                            rhs: RelationLike,
                            bindingTable: Option[BindingSet] = None)
  extends BinaryPrimitive(lhs, rhs, bindingTable)
