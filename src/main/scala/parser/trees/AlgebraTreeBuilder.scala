package parser.trees

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.trees.TreeBuilder
import parser.trees.QueryTreeBuilder.extractQueryClause

/**
  * Creates the algebraic tree from the lexical tree generated with Spoofax.
  *
  * The root of the algebra tree is a [[Query]] clause, whose children are the optional path clause
  * and the mandatory [[ConstructClause]] and [[MatchClause]].
  */
object AlgebraTreeBuilder extends TreeBuilder[SpoofaxBaseTreeNode, AlgebraTreeNode] {

  override def build(from: SpoofaxBaseTreeNode): AlgebraTreeNode = {
    extractQueryClause(from)
  }
}
