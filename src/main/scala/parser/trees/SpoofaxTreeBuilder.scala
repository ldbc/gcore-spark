package parser.trees

import common.trees.TreeBuilder
import org.spoofax.interpreter.terms.IStrategoTerm

/** Creates a [[SpoofaxBaseTreeNode]] tree from an AST of type [[IStrategoTerm]]. */
object SpoofaxTreeBuilder extends TreeBuilder[IStrategoTerm, SpoofaxBaseTreeNode] {
  override def build(from: IStrategoTerm): SpoofaxBaseTreeNode = SpoofaxTreeNode(from)
}
