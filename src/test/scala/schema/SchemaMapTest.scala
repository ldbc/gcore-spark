package schema

import ir.exceptions.IrException
import org.scalatest.FunSuite

class SchemaMapTest extends FunSuite {

  test("Union with disjoint map creates a map with key-value pairs from both sides of union") {
    val map1 = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3))
    val map2 = SchemaMap(Map("qux" -> 1, "fred" -> 2))

    val expectedUnion = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3, "qux" -> 1, "fred" -> 2))

    assert((map1 union map2) == expectedUnion)
    assert((map2 union map1) == expectedUnion)
  }

  test("Union with non-disjoint map throws exception") {
    val map1 = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3))
    val map2 = SchemaMap(Map("foo" -> 1, "fred" -> 2))

    assertThrows[IrException] {
      map1 union map2
    }
  }
}
