package algebra.operators

case class Rename(relation: RelationLike,
                  renameOperator: RenameOperator,
                  bindingContext: Option[BindingContext] = None)
  extends UnaryPrimitive(relation, bindingContext) {

  children = List(relation, renameOperator)
}

abstract class RenameOperator extends AlgebraPrimitive

case class RenameAttribute(from: Attribute, to: Attribute) extends RenameOperator {
  children = List(from, to)
}
