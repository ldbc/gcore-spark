package algebra.trees

import algebra.expressions.Reference
import org.scalatest.FunSuite

class BindingContextTest extends FunSuite {

  test("allRefs") {
    val bindingContext =
      BindingContext(
        vertexBindings = Set(Reference("v0"), Reference("v1"), Reference("v2")),
        edgeBindings = Set(ReferenceTuple(Reference("e"), Reference("v1"), Reference("v2"))),
        pathBindings = Set(ReferenceTuple(Reference("p"), Reference("v1"), Reference("v2"))))
    val expected =
      Set(Reference("v0"), Reference("v1"), Reference("v2"), Reference("e"), Reference("p"))
    val actual = bindingContext.allRefs
    assert(actual == expected)
  }
}
