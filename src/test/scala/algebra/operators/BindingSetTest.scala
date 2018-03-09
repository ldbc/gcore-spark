package algebra.operators

import algebra.expressions.Reference
import org.scalatest.FunSuite

class BindingSetTest extends FunSuite {

  test("++ with other set") {
    runUnionTest(_ ++ _)
  }

  test("+= with other set") {
    runUnionTest(_ += _)
  }

  test("foreach") {
    val traversal = new collection.mutable.ArrayBuffer[Reference]()
    val refSet = Set(Reference("foo"), Reference("bar"), Reference("baz"))
    val bset = BindingSet(refSet)

    bset.foreach(ref => traversal += ref)
    assert(traversal == refSet.toSeq)
  }

  test("intersect with other BindingSet") {
    val bset1 = new BindingSet(Reference("foo"), Reference("bar"))
    val bset2 = new BindingSet(Reference("foo"), Reference("qux"))

    assert((bset1 intersect bset2) == new BindingSet(Reference("foo")))
  }

  test("size") {
    val bset = new BindingSet(Reference("foo"), Reference("bar"), Reference("baz"))
    assert(bset.size == 3)
  }

  test("empty") {
    val bset = new BindingSet(Reference("foo"), Reference("bar"), Reference("baz"))
    val emptyBset = new BindingSet()

    assert(!bset.isEmpty)
    assert(emptyBset.isEmpty)
  }

  test("intersect sequence of BindingSets") {
    val bset1 = new BindingSet(Reference("foo"), Reference("bar"))
    val bset2 = new BindingSet(Reference("foo"), Reference("qux"))
    val bset3 = new BindingSet(Reference("foo"), Reference("qux"), Reference("quux"))

    assert(
      BindingSet.intersect(Seq(bset1, bset2, bset3)) ==
      new BindingSet(Reference("foo"), Reference("qux"))
    )
  }

  private def runUnionTest(op: (BindingSet, BindingSet) => BindingSet): Unit = {
    val bset1 = new BindingSet(Reference("foo"), Reference("bar"))
    val bset2 = new BindingSet(Reference("baz"))
    val bset3 = new BindingSet(Reference("foo"), Reference("qux"))

    assert(op(bset1, bset2) == new BindingSet(Reference("foo"), Reference("bar"), Reference("baz")))
    assert(op(bset1, bset3) == new BindingSet(Reference("foo"), Reference("bar"), Reference("qux")))
  }
}
