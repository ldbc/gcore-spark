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

package common.trees

import org.scalatest.FunSuite

/**
  * Tests for the [[TreeNode]]. Every test in this suite that changes the test tree compares the
  * pre-order traversal of the changed tree to an expected one. For the test tree used in the tests,
  * the canonical pre-order traversal is: 1, 2, 4, 5, 3.
  */
class TreeNodeTest extends FunSuite with TestTreeWrapper {

  val f: PartialFunction[IntTree, IntTree] = {
    case node => IntTree(value = node.children.map(_.value).sum + 1, descs = node.children)
  }

  test("preOrderMap") {
    val expectedInOrderTraversal: Seq[Int] = Seq(1, 2, 4, 5, 3)
    val actual = tree.preOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("postOrderMap") {
    val expectedPostOrderTraversal: Seq[Int] = Seq(4, 5, 2, 3, 1)
    val actual = tree.postOrderMap(_.value)
    assert(actual == expectedPostOrderTraversal)
  }

  test("inOrderMap") {
    val expectedInOrderTraversal: Seq[Int] = Seq(3, 4, 2, 5, 1)
    val actual = multiChildTree.inOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("transformDown") {
    val expectedInOrderTraversal: Seq[Int] = Seq(6, 10, 1, 1, 1)
    val actual = tree.transformDown(f).preOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("transformUp") {
    val expectedInOrderTraversal: Seq[Int] = Seq(5, 3, 1, 1, 1)
    val actual = tree.transformUp(f).preOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("isLeaf") {
    val expectedInOrderTraversal: Seq[Boolean] = Seq(false, false, true, true, true)
    val actual = tree.preOrderMap(_.isLeaf)
    assert(actual == expectedInOrderTraversal)
  }
}
