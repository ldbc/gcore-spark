package common.trees

import org.scalatest.FunSuite

/**
  * Tests for the [[TreeNode]]. Every test in this suite that changes the test tree compares the
  * in-order traversal of the changed tree to an expected one. For the test tree used in the tests,
  * the canonical in-order traversal is: 1, 2, 4, 5, 3.
  */
class TreeNodeTest extends FunSuite with TestTreeWrapper {

  val f: PartialFunction[IntTree, IntTree] = {
    case node => IntTree(value = node.children.map(_.value).sum + 1, descs = node.children)
  }

  test("inOrderMap") {
    val expectedInOrderTraversal: Seq[Int] = Seq(1, 2, 4, 5, 3)
    val actual = tree.inOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("postOrderMap") {
    val expectedInOrderTraversal: Seq[Int] = Seq(4, 5, 2, 3, 1)
    val actual = tree.postOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("transformDown") {
    val expectedInOrderTraversal: Seq[Int] = Seq(6, 10, 1, 1, 1)
    val actual = tree.transformDown(f).inOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("transformUp") {
    val expectedInOrderTraversal: Seq[Int] = Seq(5, 3, 1, 1, 1)
    val actual = tree.transformUp(f).inOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("isLeaf") {
    val expectedInOrderTraversal: Seq[Boolean] = Seq(false, false, true, true, true)
    val actual = tree.inOrderMap(_.isLeaf)
    assert(actual == expectedInOrderTraversal)
  }
}
