package schema

import algebra.expressions.{Label, PropertyKey}
import algebra.types.GcoreDataType
import schema.EntitySchema.{LabelRestrictionMap, LabelPropsSchemaMap}

/** The schema of a [[PathPropertyGraph]]. */
trait GraphSchema {

  def vertexSchema: EntitySchema

  def pathSchema: EntitySchema

  def edgeSchema: EntitySchema

  def edgeRestrictions: LabelRestrictionMap

  def storedPathRestrictions: LabelRestrictionMap
}

/**
  * The schema of an entity (vertex, edge or path) stores information about the key-value attributes
  * that are associated with a label. Note that this association is a [[Map]], therefore a [[Label]]
  * can/should be applied to one and only one type of entity.
  */
case class EntitySchema(labelSchema: LabelPropsSchemaMap) {

  /** All the labels in this [[EntitySchema]]. */
  def labels: Seq[Label] = labelSchema.keys

  /** All the properties in this [[EntitySchema]], mapped to the given [[Label]]. */
  def properties(label: Label): Seq[PropertyKey] = {
    val propertyMap = labelSchema.get(label)
    if (propertyMap.isDefined)
      propertyMap.get.keys
    else
      Seq.empty
  }

  /** All the properties in this [[EntitySchema]]. */
  def properties: Seq[PropertyKey] = labelSchema.values.flatMap(_.keys)

  /**
    * Creates a new [[EntitySchema]] containing the label schema in this [[EntitySchema]] and the
    * label schema in the other [[EntitySchema]].
    */
  def union(other: EntitySchema): EntitySchema =
    copy(labelSchema union other.labelSchema)

  override def toString: String =
    labelSchema.map.foldLeft(new StringBuilder) {
      case (aggStr, kv) => aggStr.append(s"\tTable ${kv._1} => ${kv._2}\n")
    }.toString()
}

object EntitySchema {
  val empty = EntitySchema(SchemaMap.empty[Label, SchemaMap[PropertyKey, GcoreDataType]])

  type LabelPropsSchemaMap = SchemaMap[Label, SchemaMap[PropertyKey, GcoreDataType]]
  type LabelRestrictionMap = SchemaMap[Label, (Label, Label)]
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
