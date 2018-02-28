package ir.algebra.types

import scala.reflect.ClassTag

abstract class DataType[T](implicit tType: ClassTag[T]) extends AlgebraType {
  override def toString: String = s"$name [${tType.toString()}]"
}

case class TypeInteger() extends DataType[Long]
case class TypeDecimal() extends DataType[Double]
case class TypeString() extends DataType[String]
case class TypeBoolean() extends DataType[Boolean]
case class TypeArray() extends DataType[Int] // this will only be the sequence for paths
case class TypeDate() extends DataType[String]
case class TypeTimestamp() extends DataType[String]
