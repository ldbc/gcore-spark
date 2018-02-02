package ir.spoofax

import ir.Node

/** A connection between two vertices. */
sealed trait VertexConn
case class OutConn() extends Node with VertexConn {
  name = "OutConn"
}
case class InConn() extends Node with VertexConn {
  name = "InConn"
}
case class InOutConn() extends Node with VertexConn {
  name = "InOutConn"
}
case class UndirectedConn() extends Node with VertexConn {
  name = "UndirectedConn"
}

case class BasicGraphPattern(vPattern: Node, evPatterns: Seq[Node]) extends Node {
  name = "BasicGraphPattern"
  children = vPattern +: evPatterns
}

case class VertexMatchPattern(varDef: Node, pattern: Node) extends Node {
  name = "VertexMatchPattern"
  children = List(varDef, pattern)

  // TODO: Add interpreter check to verify that indeed we have chosen a random name for an unnamed
  // variable.
}
case class EdgeMatchPattern(varDef: Node, pattern: Node) extends Node {
  name = "EdgeMatchPattern"
  children = List(varDef, pattern)

  // TODO: Add interpreter check to verify that indeed we have chosen a random name for an unnamed
  // variable.
}
case class PathMatchPattern() extends Node {
  name = "PathMatchPattern"
}
case class EdgeVertexMatchPattern(conn: Node, epPattern: Node, vPattern: Node) extends Node {
  // Connection, EdgeMatchPattern/PathMatchPattern + VertexMatchPattern
  name = "EdgeVertexMatchPattern"
  children = List(conn, epPattern, vPattern)
}
