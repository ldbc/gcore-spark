package algebra.types

abstract class DataType extends AlgebraType {
  type ScalaType
}

case class GcoreInteger() extends DataType { override type ScalaType = Long }
case class GcoreDecimal() extends DataType { override type ScalaType = Double }
case class GcoreString() extends DataType { override type ScalaType = String }
case class GcoreBoolean() extends DataType { override type ScalaType = Boolean }
case class GcoreArray() extends DataType { override type ScalaType = Int }
case class GcoreDate() extends DataType { override type ScalaType = String }
case class GcoreTimestamp() extends DataType { override type ScalaType = String }
