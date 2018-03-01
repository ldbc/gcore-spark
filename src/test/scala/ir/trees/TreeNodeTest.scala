package ir.trees

import org.scalatest.FunSuite

class TreeNodeTest extends FunSuite with TestTreeWrapper {

  val f: PartialFunction[IntTree, IntTree] = {
    case node => IntTree(value = node.children.map(_.value).sum + 1, descs = node.children)
  }

  test("inOrderMap") {
    val expectedInOrderTraversal: Seq[Int] = Seq(1, 2, 4, 5, 3)
    val actual = tree.inOrderMap(_.value)
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
}
