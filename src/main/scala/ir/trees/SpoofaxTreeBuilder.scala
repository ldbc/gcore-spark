package ir.trees

import org.spoofax.interpreter.terms.IStrategoTerm

object SpoofaxTreeBuilder extends Builder[IStrategoTerm, SpoofaxBaseTreeNode] {
  override def build(from: IStrategoTerm): SpoofaxBaseTreeNode = SpoofaxTreeNode(from)
}
