package algebra.trees

import algebra.expressions.Reference
import common.compiler.Context

/**
  * Holds all the variables in an algebraic sub-tree (for example, MATCH), split into their
  * respective types.
  */
case class BindingContext(vertexBindings: Set[Reference],
                          edgeBindings: Set[ReferenceTuple],
                          pathBindings: Set[ReferenceTuple]) extends Context {

  val allRefs: Set[Reference] =
    vertexBindings ++
      edgeBindings.flatMap(conn => Seq(conn.leftRef, conn.rightRef, conn.connRef)) ++
      pathBindings.flatMap(conn => Seq(conn.leftRef, conn.rightRef, conn.connRef))
}

/** The three references making up a connection (edge or path). */
case class ReferenceTuple(connRef: Reference, leftRef: Reference, rightRef: Reference)
