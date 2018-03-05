package algebra.operators

import algebra.expressions.AlgebraExpression

abstract class AlgebraPrimitive extends AlgebraOperator

case class CartesianProduct(operators: Seq[AlgebraOperator]) extends AlgebraPrimitive {
  children = operators
}

case class Filter(operator: AlgebraOperator, expr: AlgebraExpression) extends AlgebraPrimitive {
  children = List(operator, expr)
}

abstract class JoinLike(operators: Seq[AlgebraOperator]) extends AlgebraPrimitive {
  children = operators
}

case class LeftOuterJoin(operators: Seq[AlgebraOperator]) extends JoinLike(operators)
case class SemiJoin(operators: Seq[AlgebraOperator]) extends JoinLike(operators)
case class InnerJoin(operators: Seq[AlgebraOperator]) extends JoinLike(operators)
