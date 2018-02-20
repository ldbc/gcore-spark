package ir.utils

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

  def string(value: String): IStrategoTerm = {
    new StrategoString(
      /*value = */ value,
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edge(edgeMatchPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Edge",
        /*arity = */ 1),
      /*kids = */ Array(edgeMatchPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def path(pathType: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Path",
        /*arity = */ 1),
      /*kids = */ Array(pathType),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def vpath(pathQuantifier: IStrategoTerm,
            varDef: IStrategoTerm,
            pathExpression: IStrategoTerm,
            costVarDef: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Virtual",
        /*arity = */ 4),
      /*kids = */ Array(pathQuantifier, varDef, pathExpression, costVarDef),
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

  def conn(name: String, child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ name,
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def conn(name: String): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ name,
        /*arity = */ 1),
      /*kids = */ Array.empty,
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edgeVertexMatchPattern(conn: IStrategoTerm, vertex: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "EdgeVertexMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(conn, vertex),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def query(pathClause: IStrategoTerm,
            constructClause: IStrategoTerm,
            matchClause: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "BasicQuery",
        /*arity = */ 3),
      /*kids = */ Array(pathClause, constructClause, matchClause),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def matchClause(fullGraphPatternCondition: IStrategoTerm,
                  optionalClause: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Match",
        /*arity = */ 2),
      /*kids = */ Array(fullGraphPatternCondition, optionalClause),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def fullGraphPatternCondition(fullGraphPattern: IStrategoTerm,
                                where: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "FullGraphPatternCondition",
        /*arity = */ 2),
      /*kids = */ Array(fullGraphPattern, where),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def fullGraphPattern(basicGraphPatternLocations: Seq[IStrategoTerm]): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "FullGraphPattern",
        /*arity = */ 1),
      /*kids = */ Array(createStrategoList(basicGraphPatternLocations)),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def basicGraphPatternLocation(basicGraphPattern: IStrategoTerm,
                                location: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "BasicGraphPatternLocation",
        /*arity = */ 2),
      /*kids = */ Array(basicGraphPattern, location),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def basicGraphPattern(vertexMatchPattern: IStrategoTerm,
                        edgeVertexMatchPatterns: Seq[IStrategoTerm]): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "BasicGraphPattern",
        /*arity = */ 2),
      /*kids = */ Array(vertexMatchPattern, createStrategoList(edgeVertexMatchPatterns)),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def vertexMatchPattern(varDef: IStrategoTerm,
                         objectMatchPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Vertex",
        /*arity = */ 2),
      /*kids = */ Array(varDef, objectMatchPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def location(graph: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Location",
        /*arity = */ 1),
      /*kids = */ Array(graph),
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
