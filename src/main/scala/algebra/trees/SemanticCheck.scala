package algebra.trees

import algebra.expressions.ConjunctLabels
import common.compiler.Context
import schema.{EntitySchema, Catalog, GraphSchema}

/** A [[SemanticCheck]]able node implements validation rules that are checked upon creation. */
trait SemanticCheck {

  check()

  def check(): Unit
}

/**
  * A node that extends [[SemanticCheckWithContext]] implements validation rules that require an
  * outer [[Context]]. Hence, the validation cannot be performed by the node itself and needs to be
  * triggered by the provider of the [[Context]].
  */
trait SemanticCheckWithContext {

  def checkWithContext(context: Context)
}

/** Available [[Context]]s for semantic checks. */
case class QueryContext(catalog: Catalog) extends Context
case class GraphPatternContext(schema: GraphSchema, graphName: String) extends Context
case class DisjunctLabelsContext(graphName: String, schema: EntitySchema) extends Context
case class PropertyContext(graphName: String,
                           labelsExpr: Option[ConjunctLabels],
                           schema: EntitySchema) extends Context
