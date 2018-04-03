package algebra.expressions

/** Unary expressions. */
abstract class UnaryExpression(expr: AlgebraExpression, symbol: String) extends AlgebraExpression {
  children = List(expr)

  def getSymbol: String = symbol
  def getOperand: AlgebraExpression = expr
}

case class Minus(expr: AlgebraExpression) extends UnaryExpression(expr, "-")
case class Not(expr: AlgebraExpression) extends UnaryExpression(expr, "NOT")
