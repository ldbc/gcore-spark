package algebra.operators

case class UnionAll(lhs: RelationLike,
                    rhs: RelationLike,
                    bindingTable: Option[BindingSet] = None)
  extends BinaryPrimitive(lhs, rhs, bindingTable)
