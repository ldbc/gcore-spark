/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package algebra.operators

import algebra.expressions.Reference
import algebra.operators.BinaryOperator.reduceLeft
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Inside, Matchers}

@RunWith(classOf[JUnitRunner])
class BinaryOperatorTest extends FunSuite with Matchers with Inside {

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

        val btable = reduced.getBindingSet

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

        val btable = reduced.getBindingSet

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

        val btable = reduced.getBindingSet

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
    extends BinaryOperator(lhs, rhs, bindingTable)

  sealed case class SimpleRel(refs: Reference*) extends RelationLike(new BindingSet(refs: _*))
}
