package ir.spoofax

import ir.Node

/**
  * GraphQuery.BasicQuery = <<PathClause?> <ConstructClause> <MatchClause>>
  */
case class BasicQuery(pathClause: Node, constructClause: Node, matchClause: Node) extends Node {
  name = "BasicQuery"
  children = List(pathClause, constructClause, matchClause)
}
