package algebra.expressions

import algebra.types.{GcoreDataType, GcoreBoolean, GcoreInteger, GcoreString}

/** An actual value, such as strings, integers, booleans True and False, dates, etc. */
abstract class Literal[T](value: T, gcoreType: GcoreDataType) extends AlgebraExpression {
  override def toString: String = s"$name [$value, $gcoreType]"
}

case class IntLiteral(value: Int) extends Literal[Int](value, GcoreInteger)
case class StringLiteral(value: String) extends Literal[String](value, GcoreString)

abstract class BooleanLiteral(value: Boolean) extends Literal[Boolean](value, GcoreBoolean)
case object True extends BooleanLiteral(value = true)
case object False extends BooleanLiteral(value = false)
