package algebra.expressions

abstract class MathExpression(lhs: AlgebraExpression, rhs: AlgebraExpression, symbol: String)
  extends BinaryExpression(lhs, rhs, symbol)

case class Power(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends MathExpression(lhs, rhs, "POWER")
