package ir.spoofax

import ir.Node

case class VarDef(identifier: Node) extends Node {
  name = "VarDef"
  children = List(identifier)

  override def print(level: Int): Unit = {
    println(" " * level + name + " [" + identifier.asInstanceOf[Identifier].value + "]")
  }
}

case class Identifier(value: String) extends Node { // terminal
  name = "Identifier"

  // TODO: Add semantic check here to be sure the identifier really exists.
}
