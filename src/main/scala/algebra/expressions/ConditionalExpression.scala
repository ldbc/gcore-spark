package algebra.expressions

abstract class ConditionalExpression(lhs: AlgebraExpression, rhs: AlgebraExpression, symbol: String)
  extends BinaryExpression(lhs, rhs, symbol)

case class Eq(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ConditionalExpression(lhs, rhs, "=")

case class Gt(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ConditionalExpression(lhs, rhs, ">")

case class Lt(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ConditionalExpression(lhs, rhs, "<")

case class Gte(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ConditionalExpression(lhs, rhs, ">=")

case class Lte(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ConditionalExpression(lhs, rhs, "<=")

case class Neq(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ConditionalExpression(lhs, rhs, "!=")
