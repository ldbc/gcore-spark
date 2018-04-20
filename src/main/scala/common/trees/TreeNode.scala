package common.trees

import scala.reflect.ClassTag

abstract class TreeNode[T <: TreeNode[T]: ClassTag] {
  self: T =>

  def name: String = getClass.getSimpleName

  // TODO: This is a temporary workaround that allows us to (inefficiently, but more simply)
  // implement the transformDown rule. Perhaps a more efficient solution is to let the children
  // be immutable and either keep this node, if no change occurs when rewriting it, or create a
  // case-class copy of it. Right now we replace the children regardless of whether they have
  // changed or not during the rewrite.
  var children: Seq[T] = List.empty

  def isLeaf: Boolean = children.isEmpty

  override def toString: String = name

  /** Applies the rewrite rule from top (root) to bottom (leaves), creating a new tree. */
  def transformDown(rule: PartialFunction[T, T]): T = {
    // Apply rewrite rule over current node (root).
    val newSelf: T = if (rule.isDefinedAt(self)) rule(self) else self

    // Apply rewrite rule recursively over current node's children.
    val childrenLength: Int = newSelf.children.length
    if (childrenLength == 0) {
      newSelf
    } else {
      val newChildren: Array[T] = Array.ofDim[T](childrenLength)
      var i = 0
      while (i < newChildren.length) {
        newChildren(i) = newSelf.children(i).transformDown(rule)
        i += 1
      }

      newSelf.children = newChildren
      newSelf
    }
  }

  /** Applies the rewrite rule from bottom (leaves) to top (root), creating a new tree. */
  def transformUp(rule: PartialFunction[T, T]): T = {
    val childrenLength: Int = children.length
    val newChildren: Array[T] = Array.ofDim[T](childrenLength)

    // Apply rewrite rule recursively over current node's children.
    if (childrenLength != 0) {
      var i = 0
      while (i < newChildren.length) {
        newChildren(i) = children(i).transformUp(rule)
        i += 1
      }
    }

    // Change children first, in case rule depends on children.
    children = newChildren
    // Apply rewrite rule over current node (root).
    if (rule.isDefinedAt(self)) rule(self) else self
  }

  /**
    * Applies the given function in order on the tree (root, then recursively on children from left
    * to right. The result of the function is not returned to the caller.
    */
  def forEachDown(f: T => Unit): Unit = {
    f(self)
    children.foreach(_.forEachDown(f))
  }

  /**
    * Applies the given function bottom-up on the tree (recursively from children to root). The
    * result of the function is not returned to the caller.
    */
  def forEachUp(f: T => Unit): Unit = {
    children.foreach(_.forEachUp(f))
    f(self)
  }

  /**
    * Applies the given function top-down, with the following pattern: on the first half of the
    * children list, from left to right, then on root, then on the second half of the children list.
    * "Half" here means ceiling of the halved length.
    */
  def forEachInOrder(f: T => Unit): Unit = {
    val stop: Int = (children.length + 1) / 2
    for (i <- 0 until stop)
      children(i).forEachInOrder(f)

    f(self)

    for (i <- stop until children.length)
      children(i).forEachInOrder(f)
  }

  /**
    * Applies the given function pre-order on the tree (root, then recursively on children from left
    * to right). The resulting in-order traversal is returned as a sequence of the results.
    */
  def preOrderMap[U](f: T => U): Seq[U] = {
    val traversal = new collection.mutable.ArrayBuffer[U]()
    forEachDown(traversal += f(_))
    traversal
  }

  /**
    * Applies the given function post-order on the tree (recursively on children from left to right,
    * then root). The resulting post-order traversal is returned as a sequence of the results.
    */
  def postOrderMap[U](f: T => U): Seq[U] = {
    val traversal = new collection.mutable.ArrayBuffer[U]()
    forEachUp(traversal += f(_))
    traversal
  }

  /**
    * Applies the given function in-order on the tree (recursively on the first half of the children
    * list, from left to right, then root, then the second half of children list, from left to
    * right). The resulting post-order traversal is returned as a sequence of the results.
    */
  def inOrderMap[U](f: T => U): Seq[U] = {
    val traversal = new collection.mutable.ArrayBuffer[U]()
    forEachInOrder(traversal += f(_))
    traversal
  }

  /** This tree as a pretty string. */
  def treeString(depth: Int = 0): String = {
    val subTrees = children.foldLeft(new StringBuilder) {
      (agg, child) => agg.append(child.treeString(depth + 1))
    }

    s"${prefix(depth)}+ $self\n$subTrees"
  }

  def printTree(): Unit = println(treeString())

  protected def prefix(depth: Int): String = "| " * depth
}
