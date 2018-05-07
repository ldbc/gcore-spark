package parser

import compiler.ParseStage
import algebra.operators.Query
import algebra.trees.{AlgebraTreeNode, QueryContext}
import org.metaborg.spoofax.core.Spoofax
import org.slf4j.{Logger, LoggerFactory}
import org.spoofax.interpreter.terms.IStrategoTerm
import parser.trees._
import parser.utils.GcoreLang

/**
  * A [[ParseStage]] that uses [[Spoofax]] to parse an incoming query. The sub-stages of this
  * parser are:
  *
  * <ul>
  *   <li> Generate the tree of [[IStrategoTerm]]s created by [[Spoofax]] </li>
  *   <li> Rewrite this tree to name all unbounded variables. </li>
  *   <li> Build the algebraic tree from the canonicalized parse tree. </li>
  * </ul>
  */
case class SpoofaxParser(context: ParseContext) extends ParseStage {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def parse(query: String): AlgebraTreeNode = {
    val ast: IStrategoTerm = GcoreLang.parseQuery(query)
    val parseTree: SpoofaxBaseTreeNode = SpoofaxTreeBuilder build ast
    val rewriteParseTree: SpoofaxBaseTreeNode = SpoofaxCanonicalRewriter rewriteTree parseTree
    val algebraTree: AlgebraTreeNode = AlgebraTreeBuilder build rewriteParseTree
    logger.info("\n{}", algebraTree.treeString())
    algebraTree.asInstanceOf[Query].checkWithContext(QueryContext(context.catalog))
    algebraTree
  }
}
