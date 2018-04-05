package algebra.types

/** A G-CORE data type. */
abstract class GcoreDataType extends AlgebraType {

  /** The equivalent type in Scala of this [[GcoreDataType]]. */
  type ScalaType
}

/**
  * Available data types in G-CORE queries, as defined at
  * https://github.com/ldbc/ldbc_gcore_parser/blob/master/gcore-spoofax/syntax/Literals.sdf3
  */
case object GcoreInteger extends GcoreDataType { override type ScalaType = Long }
case object GcoreDecimal extends GcoreDataType { override type ScalaType = Double }
case object GcoreString extends GcoreDataType { override type ScalaType = String }
case object GcoreBoolean extends GcoreDataType { override type ScalaType = Boolean }
case object GcoreArray extends GcoreDataType { override type ScalaType = Int }
case object GcoreDate extends GcoreDataType { override type ScalaType = String }
case object GcoreTimestamp extends GcoreDataType { override type ScalaType = String }
