package algebra.operators

case class UnionAll(lhs: RelationLike,
                    rhs: RelationLike,
                    bindingTable: Option[BindingTable] = None)
  extends BinaryPrimitive(lhs, rhs, bindingTable)

case class UnionAllRelations() extends RelationLike(BindingTable.empty)
