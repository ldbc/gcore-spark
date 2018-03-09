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
                      bindingContext: Option[BindingContext] = None)
  extends UnaryPrimitive(relation, bindingContext) {

  children = List(attrSet, relation)
}
