package algebra.operators

import algebra.expressions.Reference
import algebra.operators.BinaryPrimitive.reduceLeft
import org.scalatest.{FunSuite, Inside, Matchers}

class BinaryPrimitiveTest extends FunSuite with Matchers with Inside {

  val rel1 = SimpleRel(Reference("a"))
  val rel2 = SimpleRel(Reference("b"))
  val rel3 = SimpleRel(Reference("c"))
  val rel4 = SimpleRel(Reference("d"))

  test("reduceLeft no relation") {
    assert(
      reduceLeft(Seq.empty[RelationLike], BinOp) == RelationLike.empty
    )
  }

  test("reduceLeft one relation") {
    assert(
      reduceLeft(Seq(rel1), BinOp) == rel1
    )
  }

  test("reduceLeft two relations") {
    val reduced = reduceLeft(Seq(rel1, rel2), BinOp)

    inside(reduced) {
      case BinOp(lhs, rhs, _) => {
        lhs should matchPattern { case SimpleRel(Reference("a")) => }
        rhs should matchPattern { case SimpleRel(Reference("b")) => }

        val btable = reduced.getBindingTable

        assert(btable.refSet.nonEmpty)
        assert(btable.refSet == Set(Reference("a"), Reference("b")))
      }
    }
  }

  test("reduceLeft three relations") {
    val reduced = reduceLeft(Seq(rel1, rel2, rel3), BinOp)

    inside(reduced) {
      case BinOp(lhs, rhs, _) => {
        lhs should matchPattern {
          case BinOp(SimpleRel(Reference("a")), SimpleRel(Reference("b")), _) =>
        }
        rhs should matchPattern { case SimpleRel(Reference("c")) => }

        val btable = reduced.getBindingTable

        assert(btable.refSet.nonEmpty)
        assert(btable.refSet == Set(Reference("a"), Reference("b"), Reference("c")))
      }
    }
  }

  test("reduceLeft four relations") {
    val reduced = reduceLeft(Seq(rel1, rel2, rel3, rel4), BinOp)

    inside(reduced) {
      case BinOp(lhs, rhs, _) => {
        lhs should matchPattern {
          case BinOp(
            BinOp(SimpleRel(Reference("a")), SimpleRel(Reference("b")), _),
            SimpleRel(Reference("c")), _) =>
        }
        rhs should matchPattern { case SimpleRel(Reference("d")) => }

        val btable = reduced.getBindingTable

        assert(btable.refSet.nonEmpty)
        assert(
          btable.refSet ==
            Set(Reference("a"), Reference("b"), Reference("c"), Reference("d")))
      }
    }
  }

  sealed case class BinOp(lhs: RelationLike,
                          rhs: RelationLike,
                          bindingTable: Option[BindingSet])
    extends BinaryPrimitive(lhs, rhs, bindingTable)

  sealed case class SimpleRel(refs: Reference*) extends RelationLike(new BindingSet(refs: _*))
}
