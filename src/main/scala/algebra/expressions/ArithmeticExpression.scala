package algebra.expressions

abstract class ArithmeticExpression(lhs: AlgebraExpression, rhs: AlgebraExpression, symbol: String)
  extends BinaryExpression(lhs, rhs, symbol)

case class Mul(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs, "*")

case class Div(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs, "/")

case class Mod(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs, "%")

case class Add(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs, "+")

case class Sub(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs, "-")
