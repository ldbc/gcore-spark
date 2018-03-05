package schema

import org.scalatest.FunSuite

class SchemaMapTest extends FunSuite {

  val map1 = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3))
  val map2 = SchemaMap(Map("qux" -> 1, "fred" -> 2))
  val emptyMap = SchemaMap.empty

  test("Union with disjoint map creates a map with key-value pairs from both sides of union") {
    val expectedUnion = SchemaMap(Map("foo" -> 1, "bar" -> 2, "baz" -> 3, "qux" -> 1, "fred" -> 2))
    assert((map1 union map2) == expectedUnion)
    assert((map2 union map1) == expectedUnion)
  }

  test("Union with non-disjoint map throws exception") {
    assertThrows[SchemaException] {
      map1 union map1
    }
  }

  test("get") {
    assert(map1.get("foo").isDefined)
    assert(map1.get("foo").get == 1)
    assert(map1.get("qux").isEmpty)
  }

  test("keys") {
    assert(map1.keys == Seq("foo", "bar", "baz"))
    assert(emptyMap.keys == Seq.empty)
  }

  test("values") {
    assert(map1.values == Seq(1, 2, 3))
    assert(emptyMap.values == Seq.empty)
  }
}
