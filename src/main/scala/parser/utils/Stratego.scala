package parser.utils

import org.spoofax.interpreter.terms.IStrategoTerm
import org.spoofax.terms.{StrategoAppl, StrategoConstructor, StrategoList, StrategoString}

/** Helper object for creating [[IStrategoTerm]] subtrees. */
object Stratego {
  def objectMatchPattern(labelPreds: IStrategoTerm, propsPreds: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "ObjectMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(labelPreds, propsPreds),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def objectConstructPattern(labelAssignments: IStrategoTerm,
                             propAssignments: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "ObjectConstructPattern",
        /*arity = */ 2),
      /*kids = */ Array(labelAssignments, propAssignments),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def none: IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "None",
        /*arity = */ 0),
      /*kids = */ Array.empty[IStrategoTerm],
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def some(child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Some",
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def varDef(str: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "VarDef",
        /*arity = */ 1),
      /*kids = */ Array(str),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def varRefDef(str: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "VarRefDef",
        /*arity = */ 1),
      /*kids = */ Array(str),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def string(value: String): IStrategoTerm = {
    new StrategoString(
      /*value = */ value,
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edge(edgePattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Edge",
        /*arity = */ 1),
      /*kids = */ Array(edgePattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edgeMatchPattern(varDef: IStrategoTerm, objMatchPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "EdgeMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(varDef, objMatchPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edgeConstructPattern(varRefDef: IStrategoTerm,
                           copyPattern: IStrategoTerm,
                           groupDeclaration: IStrategoTerm,
                           objConstructPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "EdgeConstructPattern",
        /*arity = */ 4),
      /*kids = */ Array(varRefDef, copyPattern, groupDeclaration, objConstructPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def groupDeclaration(exprs: Seq[IStrategoTerm]): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "GroupDeclaration",
        /*arity = */ 1),
      /*kids = */ Array(createStrategoList(exprs)),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def conn(name: String, child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ name,
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  private def createStrategoList(elems: Seq[IStrategoTerm]): StrategoList = {
    elems match {
      case Seq(elem, other@_*) =>
        new StrategoList(
          /*head =*/ elem,
          /*tail =*/ createStrategoList(other),
          /*annotations =*/ null,
          /*storageType =*/ 0)
      case _ =>
        new StrategoList(
          /*head =*/ null,
          /*tail =*/ null,
          /*annotations =*/ null,
          /*storageType =*/ 0)
    }
  }
}
