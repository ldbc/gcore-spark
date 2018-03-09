package algebra.operators

import algebra.expressions.AlgebraExpression

case class Select(relation: RelationLike,
                  expr: AlgebraExpression,
                  bindingContext: Option[BindingContext] = None)
  extends UnaryPrimitive(relation, bindingContext) {

  children = List(relation, expr)
}
