package schema

import ir.algebra.expressions.{Label, PropertyKey}
import ir.algebra.types.{TypeInteger, TypeString}
import org.scalatest.FunSuite

class EntitySchemaTest extends FunSuite {

  test("Union with other schema creates a schema with key-value pairs from both sides of union") {
    val schema1 = EntitySchema(SchemaMap(Map(
      Label("person") -> SchemaMap(Map(
        PropertyKey("age") -> TypeInteger(),
        PropertyKey("address") -> TypeString())),
      Label("city") -> SchemaMap(Map(
        PropertyKey("population") -> TypeInteger()))
    )))
    val schema2 = EntitySchema(SchemaMap(Map(
      Label("car") -> SchemaMap(Map(
        PropertyKey("type") -> TypeString(),
        PropertyKey("manufacturer") -> TypeString()))
    )))

    val expectedSchema = EntitySchema(SchemaMap(Map(
      Label("person") -> SchemaMap(Map(
        PropertyKey("age") -> TypeInteger(),
        PropertyKey("address") -> TypeString())),
      Label("city") -> SchemaMap(Map(
        PropertyKey("population") -> TypeInteger())),
      Label("car") -> SchemaMap(Map(
        PropertyKey("type") -> TypeString(),
        PropertyKey("manufacturer") -> TypeString()))
    )))

    assert((schema1 union schema2) == expectedSchema)
  }

}
