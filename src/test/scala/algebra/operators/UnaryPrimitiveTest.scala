package algebra.operators

import algebra.expressions.Reference
import org.scalatest.FunSuite

class UnaryPrimitiveTest extends FunSuite {

  test("BindingSet of RelationLike becomes BindingSet of UnaryPrimitive") {
    val bset = new BindingSet(Reference("ref1"), Reference("ref2"))
    val op = AUnaryPrimitive(ARelationLike(bset), bindingSet = None)

    assert(op.getBindingSet.refSet == bset.refSet)
  }

  test("BindingSet of RelationLike is overriden if explicitly provided") {
    val bset = new BindingSet(Reference("ref1"), Reference("ref2"))
    val overrideBset = new BindingSet(Reference("ref3"))
    val op = AUnaryPrimitive(ARelationLike(bset), bindingSet = Some(overrideBset))

    assert(op.getBindingSet.refSet == overrideBset.refSet)
  }

  sealed case class AUnaryPrimitive(relation: RelationLike, bindingSet: Option[BindingSet] = None)
    extends UnaryPrimitive(relation, bindingSet)

  sealed case class ARelationLike(bindingSet: BindingSet) extends RelationLike(bindingSet)
}
