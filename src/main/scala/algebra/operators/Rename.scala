package algebra.operators

case class Rename(relation: RelationLike,
                  renameOperator: RenameOperator,
                  bindingTable: Option[BindingTable] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  children = List(relation, renameOperator)
}

abstract class RenameOperator extends AlgebraPrimitive

case class RenameAttribute(from: Attribute, to: Attribute) extends RenameOperator {
  children = List(from, to)
}
