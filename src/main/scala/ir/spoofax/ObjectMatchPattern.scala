package ir.spoofax

import ir.Node

case class ObjectMatchPattern(labelPreds: Node, propPreds: Node) extends Node {
  name = "ObjectMatchPattern"
  children = List(labelPreds, propPreds)
}

case class LabelPredicates(disjs: Seq[Node]) extends Node { // conjunct list of disjunctions
  name = "LabelPredicates"
  children = disjs

  // TODO: Add semantic check here to verify we only allow one conjunction - since we only allow one
  // single label per entity.
}
case class DisjunctLabels(labels: Seq[Node]) extends Node { // disjunct list of labels
  name = "DisjunctLabels"
  children = labels
}
case class Label(identifier: Node) extends Node {
  name = "Label"
  children = List(identifier)

  override def print(level: Int): Unit = {
    println(" " * level + name + " [" + identifier.asInstanceOf[Identifier].value + "]")
  }
}

case class PropertyPredicates(propPreds: Seq[Node]) extends Node { // list of Property
  name = "PropertyPredicates"
  children = propPreds
}
case class Property(identifier: Node, exp: Node) extends Node {
  name = "Property"
  children = List(identifier, exp)

  override def print(level: Int): Unit = {
    println(" " * level + name + " [" + identifier.asInstanceOf[Identifier].value + "]")
    children.tail.foreach(child => child.print(level + 2))
  }
}
