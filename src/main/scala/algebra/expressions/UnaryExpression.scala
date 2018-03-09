package algebra.expressions

/** Unary expressions. */
abstract class UnaryExpression(expr: AlgebraExpression) extends AlgebraExpression {
  children = List(expr)
}

case class Minus(expr: AlgebraExpression) extends UnaryExpression(expr)
case class Not(expr: AlgebraExpression) extends UnaryExpression(expr)
