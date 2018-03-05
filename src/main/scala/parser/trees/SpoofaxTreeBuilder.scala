package parser.trees

import common.trees.Builder
import org.spoofax.interpreter.terms.IStrategoTerm

/** Creates a [[SpoofaxBaseTreeNode]] tree from an AST of type [[IStrategoTerm]]. */
object SpoofaxTreeBuilder extends Builder[IStrategoTerm, SpoofaxBaseTreeNode] {
  override def build(from: IStrategoTerm): SpoofaxBaseTreeNode = SpoofaxTreeNode(from)
}
