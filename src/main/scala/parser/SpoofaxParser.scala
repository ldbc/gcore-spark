package parser

import api.compiler.ParseStage
import ir.trees.AlgebraTreeNode
import org.metaborg.spoofax.core.Spoofax
import org.spoofax.interpreter.terms.IStrategoTerm
import parser.trees.{AlgebraTreeBuilder, SpoofaxBaseTreeNode, SpoofaxCanonicalRewriter, SpoofaxTreeBuilder}
import parser.utils.GcoreLang

/**
  * A [[ParseStage]] that uses [[Spoofax]] to parse an incoming query. The sub-stages of this
  * parser are:
  *
  * <ul>
  *   <li> Generate the tree of [[IStrategoTerm]]s created by [[Spoofax]]; </li>
  *   <li> Rewrite this tree to name all unbounded variables. </li>
  *   <li> Build the algebraic tree from the canonicalized parse tree. </li>
  * </ul>
  */
object SpoofaxParser extends ParseStage {

  override def parse(query: String): AlgebraTreeNode = {
    val ast: IStrategoTerm = GcoreLang.parseQuery(query)
    val parseTree: SpoofaxBaseTreeNode = SpoofaxTreeBuilder build ast
    val rewriteParseTree: SpoofaxBaseTreeNode = SpoofaxCanonicalRewriter rewriteTree parseTree
    val algebraTree: AlgebraTreeNode = AlgebraTreeBuilder build rewriteParseTree
    algebraTree
  }
}
