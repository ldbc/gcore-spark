package common.trees

import org.scalatest.FunSuite

class TreeRewriterTest extends FunSuite with TestTreeWrapper {

  val fEven: PartialFunction[IntTree, IntTree] = {
    case node if node.value % 2 == 0 =>
      IntTree(value = node.children.map(_.value).sum + node.value / 2, descs = node.children)
  }

  val fOdd: PartialFunction[IntTree, IntTree] = {
    case node if node.value % 2 == 1 =>
      IntTree(value = node.children.map(_.value).sum + node.value, descs = node.children)
  }

  val f: PartialFunction[IntTree, IntTree] = fEven orElse fOdd

  test("topDownRewriter") {
    val expectedInOrderTraversal: Seq[Int] = Seq(6, 10, 2, 5, 3)
    val rewriter = new TopDownRewriter[IntTree] { override val rule = f }
    val actual = rewriter.rewriteTree(tree).preOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }

  test("bottomUpRewriter") {
    val expectedInOrderTraversal: Seq[Int] = Seq(12, 8, 2, 5, 3)
    val rewriter = new BottomUpRewriter[IntTree] { override val rule = f }
    val actual = rewriter.rewriteTree(tree).preOrderMap(_.value)
    assert(actual == expectedInOrderTraversal)
  }
}
