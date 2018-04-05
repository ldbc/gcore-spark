package algebra.expressions

abstract class PredicateExpression(expr: AlgebraExpression, symbol: String)
  extends UnaryExpression(expr, symbol)

case class IsNull(expr: AlgebraExpression) extends PredicateExpression(expr, "IS NULL")

case class IsNotNull(expr: AlgebraExpression) extends PredicateExpression(expr, "IS NOT NULL")
