package algebra.operators

import algebra.expressions.Reference
import org.scalatest.FunSuite

class UnaryOperatorTest extends FunSuite {

  test("BindingSet of RelationLike becomes BindingSet of UnaryOperator") {
    val bset = new BindingSet(Reference("ref1"), Reference("ref2"))
    val op = AUnaryOperator(ARelationLike(bset), bindingSet = None)

    assert(op.getBindingSet.refSet == bset.refSet)
  }

  test("BindingSet of RelationLike is overriden if explicitly provided") {
    val bset = new BindingSet(Reference("ref1"), Reference("ref2"))
    val overrideBset = new BindingSet(Reference("ref3"))
    val op = AUnaryOperator(ARelationLike(bset), bindingSet = Some(overrideBset))

    assert(op.getBindingSet.refSet == overrideBset.refSet)
  }

  sealed case class AUnaryOperator(relation: RelationLike, bindingSet: Option[BindingSet] = None)
    extends UnaryOperator(relation, bindingSet)

  sealed case class ARelationLike(bindingSet: BindingSet) extends RelationLike(bindingSet)
}
