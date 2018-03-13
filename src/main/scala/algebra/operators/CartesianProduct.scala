package algebra.operators

case class CartesianProduct(lhs: RelationLike,
                            rhs: RelationLike,
                            bindingTable: Option[BindingTable] = None)
  extends BinaryPrimitive(lhs, rhs, bindingTable)
