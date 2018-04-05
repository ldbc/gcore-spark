package algebra.operators

import algebra.expressions.Reference
import algebra.operators.BinaryOperator.reduceLeft
import org.scalatest.FunSuite

class JoinLikeTest extends FunSuite {

  /**       rel1 | rel2 | rel3 | rel4 | rel5
    *      +-----+------+------+------+-----+
    * a => |  y  |      |      |      |     +
    *      +-----+------+------+------+-----+
    * b => |  y  |   y  |      |      |  y  +
    *      +-----+------+------+------+-----+
    * c => |     |   y  |      |      |     +
    *      +-----+------+------+------+-----+
    * d => |     |   y  |      |  y   |     +
    *      +-----+------+------+------+-----+
    * e => |     |      |   y  |      |     +
    *      +-----+------+------+------+-----+
    * f => |     |      |   y  |      |     +
    *      +-----+------+------+------+-----+
    * g => |     |      |      |  y   |  y  +
    *      +-----+------+------+------+-----+
    */
  val rel1 = SimpleRel(Reference("a"), Reference("b"))
  val rel2 = SimpleRel(Reference("b"), Reference("c"), Reference("d"))
  val rel3 = SimpleRel(Reference("e"), Reference("f"))
  val rel4 = SimpleRel(Reference("d"), Reference("g"))
  val rel5 = SimpleRel(Reference("b"), Reference("g"))

  test("Common bindings of two joined relations") {
    val joined = AJoin(rel1, rel2)
    assert(joined.commonInSeenBindingSets == Set(Reference("b")))
  }

  test("Common bindings of reduced relations") {
    val joined = reduceLeft(Seq(rel1, rel2, rel3, rel4, rel5), AJoin).asInstanceOf[AJoin]
    assert(
      joined.commonInSeenBindingSets ==
        Set(Reference("b"), Reference("d"), Reference("g")))
  }

  sealed case class AJoin(lhs: RelationLike,
                          rhs: RelationLike,
                          bindingTable: Option[BindingSet] = None)
    extends JoinLike(lhs, rhs, bindingTable)

  sealed case class SimpleRel(refs: Reference*) extends RelationLike(new BindingSet(refs: _*))
}
