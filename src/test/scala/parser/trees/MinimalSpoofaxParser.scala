package parser.trees

import algebra.trees.AlgebraTreeNode
import parser.utils.GcoreLang

trait MinimalSpoofaxParser {

  def parse(query: String): AlgebraTreeNode = {
    val ast = GcoreLang parseQuery query
    val spoofaxTree = SpoofaxTreeBuilder build ast
    val canonicalSpoofaxTree = SpoofaxCanonicalRewriter rewriteTree spoofaxTree
    val algebraTree = AlgebraTreeBuilder build canonicalSpoofaxTree
    algebraTree
  }

  /**
    * Uses the [[SpoofaxTreeBuilder]] to create the parse tree from a canonical parse tree. Also
    * calls the [[SpoofaxCanonicalRewriter]].
    */
  def canonicalize(query: String): SpoofaxBaseTreeNode = {
    val ast = GcoreLang parseQuery query
    val spoofaxTree = SpoofaxTreeBuilder build ast
    val canonicalTree = SpoofaxCanonicalRewriter rewriteTree spoofaxTree
    canonicalTree
  }
}
