package algebra.operators

import algebra.expressions.Reference

case class Attribute(reference: Reference) extends AlgebraPrimitive {
  children = List(reference)

  def this(ref: String) = this(Reference(ref))
}

case class AttributeSet(attrs: Attribute*) extends AlgebraPrimitive {
  children = attrs
}

case class Projection(attrSet: AttributeSet,
                      relation: RelationLike,
                      bindingTable: Option[BindingTable] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  children = List(attrSet, relation)
}
