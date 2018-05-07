package algebra.trees

import algebra.expressions.Reference
import algebra.types.Graph
import common.compiler.Context
import schema.Catalog

/** A [[Context]] used by the algebraic rewriters. */
case class AlgebraContext(catalog: Catalog,
                          bindingToGraph: Option[Map[Reference, Graph]] = None,
                          bindingContext: Option[BindingContext] = None)
  extends Context
