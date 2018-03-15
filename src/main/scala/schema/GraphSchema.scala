package schema

import algebra.expressions.{Label, PropertyKey}
import algebra.types.DataType

/** The schema of a [[PathPropertyGraph]]. */
trait GraphSchema {

  def vertexSchema: EntitySchema

  def pathSchema: EntitySchema

  def edgeSchema: EntitySchema

  def edgeRestrictions: SchemaMap[Label, (Label, Label)]
}

/**
  * The schema of an entity (vertex, edge or path) stores information about the key-value attributes
  * that are associated with a label. Note that this association is a [[Map]], therefore a [[Label]]
  * can/should be applied to one and only one type of entity.
  */
case class EntitySchema(labelSchema: SchemaMap[Label, SchemaMap[PropertyKey, DataType[_]]]) {

  def labels: Seq[Label] = labelSchema.keys

  def properties(label: Label): Seq[PropertyKey] = {
    val propertyMap = labelSchema.get(label)
    if (propertyMap.isDefined)
      propertyMap.get.keys
    else
      Seq.empty
  }

  def properties: Seq[PropertyKey] = labelSchema.values.flatMap(_.keys)

  def union(other: EntitySchema): EntitySchema =
    copy(labelSchema union other.labelSchema)

  override def toString: String =
    labelSchema.map.foldLeft(new StringBuilder) {
      case (aggStr, kv) => aggStr.append(s"\tTable ${kv._1} => ${kv._2}\n")
    }.toString()
}

object EntitySchema {
  val empty = EntitySchema(SchemaMap.empty[Label, SchemaMap[PropertyKey, DataType[_]]])
}

/** A general-purpose [[Map]] with richer union semantics. */
case class SchemaMap[K, V](map: Map[K, V]) {

  def get(key: K): Option[V] = map.get(key)

  def keys: Seq[K] = map.keys.toSeq

  def values: Seq[V] = map.values.toSeq

  def union(other: SchemaMap[K, V]): SchemaMap[K, V] = {
    val newMap = other.map.foldLeft(map) {
      case (currMap, (k, v)) =>
        if (currMap.contains(k))
          throw SchemaException(s"Key $k with value $v is already present in this SchemaMap.")
        else
          currMap + (k -> v)
    }

    copy(map = newMap)
  }

  override def toString: String = s"$map"
}

object SchemaMap {
  def empty[K, V] = SchemaMap(Map.empty[K, V])
}
