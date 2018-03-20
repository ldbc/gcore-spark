package planner.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{Relation, VertexRelation}
import algebra.types.Graph
import planner.trees.PlannerContext

case class VertexScan(vertexRelation: VertexRelation, graph: Graph, context: PlannerContext)
  extends EntityScan(graph, context) {

  val tableName: Label = vertexRelation.labelRelation.asInstanceOf[Relation].label
  private val columnConditions: AlgebraExpression = vertexRelation.expr

  val binding: Reference = vertexRelation.ref

//  val scanOperation: TableOperator = Select(tableName, columnConditions)

  children = List(binding) //, scanOperation)
}
