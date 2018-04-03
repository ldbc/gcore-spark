package algebra.expressions

/** Binary expressions. */
abstract class BinaryExpression(lhs: AlgebraExpression, rhs: AlgebraExpression, symbol: String)
  extends AlgebraExpression {

  children = List(lhs, rhs)

  def getSymbol: String = symbol
  def getLhs: AlgebraExpression = lhs
  def getRhs: AlgebraExpression = rhs
}
