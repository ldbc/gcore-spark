package ir.trees

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

  // TODO: Remove this field, it is unnecessary, given that a leaf has children = List.empty
  def isLeaf: Boolean = false

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

  def forEach(f: T => Unit): Unit = {
    f(self)
    children.foreach(_.forEach(f))
  }

  def inOrderMap[U](f: T => U): Seq[U] = {
    val traversal = new collection.mutable.ArrayBuffer[U]()
    forEach(traversal += f(_))
    traversal
  }

  def treeString(implicit depth:Int = 0): String = {
    val subTrees = children.foldLeft(new StringBuilder) {
      (agg, child) => agg.append(child.treeString(depth + 1))
    }

    s"${prefix(depth)}$self\n$subTrees"
  }

  protected def prefix(depth: Int): String = "· " * depth
}
