package algebra.expressions

abstract class LogicalExpression(lhs: AlgebraExpression, rhs: AlgebraExpression, symbol: String)
  extends BinaryExpression(lhs, rhs, symbol)

case class And(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends LogicalExpression(lhs, rhs, "AND")

case class Or(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends LogicalExpression(lhs, rhs, "OR")
