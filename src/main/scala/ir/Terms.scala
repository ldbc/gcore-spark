package ir

trait Node {
  var name: String = "None"
  var children: Seq[Node] = List.empty

  def print(level: Int = 0): Unit = {
    println(" " * level + name)
    children.foreach(child => child.print(level + 2))
  }
}

/** The empty node. */
case class None() extends Node
