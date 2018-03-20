package algebra.operators

import algebra.expressions.Reference
import org.scalatest.FunSuite

class BindingTableTest extends FunSuite {

  test("++ with other table") {
    val btable1 = new BindingSet(Reference("foo"), Reference("bar"))
    val btable2 = new BindingSet(Reference("baz"))
    val btable3 = new BindingSet(Reference("foo"), Reference("qux"))

    assert(btable1 ++ btable2 ===
      new BindingSet(Reference("foo"), Reference("bar"), Reference("baz")))
    assert(btable1 ++ btable3 ===
      new BindingSet(Reference("foo"), Reference("bar"), Reference("qux")))
  }

  test("intersect sequence of BindingTables") {
    val bset1 = Set(Reference("foo"), Reference("bar"))
    val bset2 = Set(Reference("foo"), Reference("qux"))
    val bset3 = Set(Reference("foo"), Reference("qux"), Reference("quux"))

    assert(
      BindingSet.intersectBindingTables(Seq(bset1, bset2, bset3)) ==
      Set(Reference("foo"), Reference("qux"))
    )
  }
}
