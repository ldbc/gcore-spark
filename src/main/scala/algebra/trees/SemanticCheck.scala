package algebra.trees

import algebra.expressions.WithLabels
import common.compiler.Context
import schema.{EntitySchema, GraphDb, GraphSchema}

trait SemanticCheck {

  check()

  def check(): Unit
}

trait SemanticCheckWithContext {

  def checkWithContext(context: Context)
}

case class QueryContext(graphDb: GraphDb[_]) extends Context
case class GraphPatternContext(schema: GraphSchema, graphName: String) extends Context
case class DisjunctLabelsContext(graphName: String, schema: EntitySchema) extends Context
case class PropertyContext(graphName: String, labelsExpr: Option[WithLabels], schema: EntitySchema)
  extends Context
