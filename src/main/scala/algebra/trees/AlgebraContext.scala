package algebra.trees

import algebra.expressions.Reference
import algebra.types.Graph
import common.compiler.Context
import schema.GraphDb

case class AlgebraContext(graphDb: GraphDb, bindingToGraph: Option[Map[Reference, Graph]] = None)
  extends Context
