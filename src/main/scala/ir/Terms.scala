package ir

sealed trait Node {
  var name: String = "None"
  var children: Seq[Node] = List.empty

  def print(level: Int = 0): Unit = {
    println(" " * level + name)
    children.foreach(child => child.print(level + 2))
  }
}

// A connection between two vertices.
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

/** The empty node. */
case class None() extends Node

/**
  * GraphQuery.BasicQuery = <<PathClause?> <ConstructClause> <MatchClause>>
  */
case class BasicQuery(pathClause: Node, constructClause: Node, matchClause: Node) extends Node {
  name = "BasicQuery"
  children = List(pathClause, constructClause, matchClause)
}

/**
  * MatchClause.Match = <MATCH <FullGraphPatternCondition> <OptionalClause?>
  * FullGraphPatternCondition.FullGraphPatternCondition = <<FullGraphPattern> <WhereClause?>>
  * OptionalClause.OptionalClause = <<{Optional "\n"}+>>
  * Optional.Optional = <OPTIONAL <FullGraphPatternCondition>>
  *
  * MATCH over a sequence of FullGraphPatternCondition clauses. The first one is the pattern given
  * to the MATCH directly and, if there is more than one, the rest correspond to the patterns under
  * the OPTIONAL clauses.
  */
case class MatchClause(patterns: Seq[Node]) extends Node {
  name = "MatchClause"
  children = patterns
}
case class FullGraphPatternCondition(patterns: Node, // FullGraphPattern
                                     whereClause: Node,
                                     isOptional: Boolean = false) extends Node {
  name = "FullGraphPatternCondition"
  children = List(patterns, whereClause)

  override def print(level: Int): Unit = {
    val optional: String = if (isOptional) " [optional]" else ""
    println(" " * level + name + optional)
    children.foreach(child => child.print(level + 2))
  }
}

/**
  * FullGraphPatternCondition.FullGraphPatternCondition = <<FullGraphPattern> <WhereClause?>>
  * FullGraphPattern.FullGraphPattern = <<{BasicGraphPatternLocation ",\n"}+>>
  * WhereClause.Where = <WHERE <Exp>> {case-insensitive}
  *
  * We express FullGraphPatternCondition as:
  *   FullGraphPatternCondition = (
  *     FullGraphPattern = List(BasicGraphPatternLocation = Tuple(BasicGraphPattern, Location)),
  *     WhereClause
  *   )
  *
  * BasicGraphPattern.BasicGraphPattern = <<VertexMatchPattern> <EdgeVertexMatchPattern*>>
  */
case class FullGraphPattern(patterns: Seq[Node]) extends Node { // Seq[BasicGraphPatternLocation]
  name = "FullGraphPattern"
  children = patterns
}
case class BasicGraphPatternLocation(pattern: Node, location: Node) extends Node {
  name = "BasicGraphPatternLocation"
  children = List(pattern, location)
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
case class VarDef(identifier: Node) extends Node {
  name = "VarDef"
  children = List(identifier)

  override def print(level: Int): Unit = {
    println(" " * level + name + " [" + identifier.asInstanceOf[Identifier].value + "]")
  }
}
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
case class Identifier(value: String) extends Node { // terminal
  name = "Identifier"

  // TODO: Add semantic check here to be sure the identifier really exists.
}
case class PropertyPredicates(propPreds: Seq[Node]) extends Node { // list of Property
  name = "PropertyPredicates"
  children = propPreds
}
case class Property(identifier: Node, exp: Expression) extends Node {
  name = "Property"
  children = List(identifier, exp)

  override def print(level: Int): Unit = {
    println(" " * level + name + " [" + identifier.asInstanceOf[Identifier].value + "]")
    children.tail.foreach(child => child.print(level + 2))
  }
}
case class Expression() extends Node {
  name = "Expression"
}
case class Location() extends Node {
  name = "Location"
}
case class WhereClause() extends Node {
  name = "WhereClause"
}
