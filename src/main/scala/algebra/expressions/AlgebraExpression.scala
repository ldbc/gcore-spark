package algebra.expressions

import algebra.operators.Query
import algebra.trees._

/**
  * A G-CORE expressions as defined at
  * https://github.com/ldbc/ldbc_gcore_parser/blob/master/gcore-spoofax/syntax/Expressions.sdf3.
  */
abstract class AlgebraExpression extends AlgebraTreeNode

case class True() extends AlgebraExpression
case class False() extends AlgebraExpression

/** An entity (vertex, edge, path) name. */
case class Reference(refName: String) extends AlgebraExpression {
  override def toString: String = s"$name [$refName]"
}

/** An actual value, such as strings, integers, booleans True and False, dates, etc. */
case class Literal[T](literalValue: T) extends AlgebraExpression {
  override def toString: String = s"$name [$literalValue]"
}

case class Exists(query: Query) extends AlgebraExpression {
  children = List(query)
}
