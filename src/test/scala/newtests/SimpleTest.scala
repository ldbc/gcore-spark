package newtests

import org.scalatest.{FunSuite}
import scala.collection.mutable.Stack

class SimpleTest extends FunSuite {
  test("A simple test"){
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    val oldSize = stack.size
    val result = stack.pop()
    assert(result === 2)
    assert(stack.size === oldSize - 1)
  }
}
