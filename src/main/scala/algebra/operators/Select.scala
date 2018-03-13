package algebra.operators

import algebra.expressions.AlgebraExpression

case class Select(relation: RelationLike,
                  expr: AlgebraExpression,
                  bindingTable: Option[BindingTable] = None)
  extends UnaryPrimitive(relation, bindingTable) {

  children = List(relation, expr)
}
