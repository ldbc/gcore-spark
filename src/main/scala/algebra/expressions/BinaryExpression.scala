package algebra.expressions

/** Binary expressions. */
abstract class BinaryExpression(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends AlgebraExpression {

  children = List(lhs, rhs)
}

case class ObjectPattern(labelsPred: AlgebraExpression, propsPred: AlgebraExpression)
  extends BinaryExpression(labelsPred, propsPred)

case class In(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends BinaryExpression(lhs, rhs)

case class And(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)

case class Or(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
