package algebra.target_api

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{Relation, VertexRelation}
import algebra.types.Graph
import schema.Catalog

abstract class VertexScan(vertexRelation: VertexRelation, graph: Graph, catalog: Catalog)
  extends EntityScan(graph, catalog) {

  val tableName: Label = vertexRelation.labelRelation.asInstanceOf[Relation].label
  val columnConditions: AlgebraExpression = vertexRelation.expr

  val binding: Reference = vertexRelation.ref

  children = List(binding, tableName, columnConditions)
}
