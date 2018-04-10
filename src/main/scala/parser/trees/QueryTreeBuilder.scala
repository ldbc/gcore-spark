package parser.trees

import algebra.operators._
import parser.exceptions.QueryParseException
import parser.trees.ConstructTreeBuilder._
import parser.trees.MatchTreeBuilder._

object QueryTreeBuilder {

  def extractQueryClause(from: SpoofaxBaseTreeNode): Query = {
    from.name match {
      case "BasicQuery" =>
        //        val pathClause = extractClause(from.children.head)
        val constructClause = extractClause(from.children(1)).asInstanceOf[ConstructClause]
        val matchClause = extractClause(from.children(2)).asInstanceOf[MatchClause]

        Query(constructClause, matchClause)

      case _ => throw QueryParseException(s"Query type ${from.name} unsupported for the moment.")
    }
  }

  /**
    * Builds the subtree of one of the possible clauses in a [[Query]]: path, [[ConstructClause]]
    * and [[MatchClause]].
    */
  private def extractClause(from: SpoofaxBaseTreeNode): AlgebraOperator = {
    from.name match {
      case "Construct" => extractConstructClause(from)
      case "Match" => extractMatchClause(from)
      case _ => throw QueryParseException(s"Cannot extract clause from node type ${from.name}")
    }
  }
}
