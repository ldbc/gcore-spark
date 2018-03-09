package algebra.expressions

abstract class ArithmeticExpression(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends BinaryExpression(lhs, rhs)

case class Power(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Mul(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Div(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Mod(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Add(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Sub(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Eq(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Gt(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Lt(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Gte(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Lte(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)

case class Neq(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends ArithmeticExpression(lhs, rhs)
