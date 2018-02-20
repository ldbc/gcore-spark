package ir.trees

import org.spoofax.interpreter.terms.IStrategoTerm.{APPL, INT, LIST, STRING}
import org.spoofax.interpreter.terms.{IStrategoAppl, IStrategoInt, IStrategoString, IStrategoTerm}

/** A node in a Spoofax parse tree. */
abstract class SpoofaxBaseTreeNode(term: IStrategoTerm) extends TreeNode[SpoofaxBaseTreeNode] {

  // This assumes we have no nested lists, like for eg:
  // term children = [APPL, [APPL, LIST], APPL]
  // After unwrapping the list above, we get: [APPL, APPL, LIST, APPL], so the second list remainds unwrapped.
  // TODO: Do we have any cases of nested lists?
  children = {
    term
      .getAllSubterms
      .toList
      .flatMap(subTerm => {
        subTerm.getTermType match {
          case LIST => subTerm.getAllSubterms.toList
          case _ => List(subTerm)
        }
      })
      .map(term => term.getTermType match {
        case APPL => SpoofaxTreeNode(term)
        case INT => SpoofaxLeaf[Integer](term, term.asInstanceOf[IStrategoInt].intValue())
        case STRING => SpoofaxLeaf[String](term, term.asInstanceOf[IStrategoString].stringValue())
      })
  }
}

case class SpoofaxTreeNode(term: IStrategoTerm) extends SpoofaxBaseTreeNode(term) {
  override def name: String = term.asInstanceOf[IStrategoAppl].getConstructor.getName
}

case class SpoofaxLeaf[ValueType](term: IStrategoTerm, leafValue: ValueType)
  extends SpoofaxBaseTreeNode(term) {

  override def isLeaf: Boolean = true

  children = List.empty

  def value: ValueType = leafValue

  override def toString: String = s"$name [$value]"
}
