package algebra.expressions

/** Predicate expressions. */
abstract class PredicateExpression(expr: AlgebraExpression) extends UnaryExpression(expr)

case class IsNull(expr: AlgebraExpression) extends PredicateExpression(expr)

case class IsNotNull(expr: AlgebraExpression) extends PredicateExpression(expr)
