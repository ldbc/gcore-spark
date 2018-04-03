package algebra.operators

import algebra.expressions.AlgebraExpression

case class Select(relation: RelationLike,
                  expr: AlgebraExpression,
                  bindingSet: Option[BindingSet] = None)
  extends UnaryPrimitive(relation, bindingSet) {

  children = List(relation, expr)
}
