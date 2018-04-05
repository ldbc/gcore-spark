package algebra.operators

import algebra.expressions.AlgebraExpression

/**
  * The relational selection that restricts a [[RelationLike]] to a subset of it. Given that the
  * header of the binding table is not changed during a selection, the [[BindingSet]] remains
  * unchanged. The selection predicate is an [[AlgebraExpression]].
  */
case class Select(relation: RelationLike,
                  expr: AlgebraExpression,
                  bindingSet: Option[BindingSet] = None)
  extends UnaryOperator(relation, bindingSet) {

  children = List(relation, expr)
}
