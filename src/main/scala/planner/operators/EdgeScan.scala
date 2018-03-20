package planner.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{EdgeRelation, Relation}
import algebra.types.Graph
import planner.trees.PlannerContext

case class EdgeScan(edgeRelation: EdgeRelation, graph: Graph, context: PlannerContext)
  extends EntityScan(graph, context) {

  val edgeTableName: Label = edgeRelation.labelRelation.asInstanceOf[Relation].label
  val fromTableName: Label = edgeRelation.fromRel.labelRelation.asInstanceOf[Relation].label
  val toTableName: Label = edgeRelation.toRel.labelRelation.asInstanceOf[Relation].label

  private val edgeExpr: AlgebraExpression = edgeRelation.expr
  private val fromExpr: AlgebraExpression = edgeRelation.fromRel.expr
  private val toExpr: AlgebraExpression = edgeRelation.toRel.expr

  val edgeBinding: Reference = edgeRelation.ref
  val fromBinding: Reference = edgeRelation.fromRel.ref
  val toBinding: Reference = edgeRelation.toRel.ref
//
//  val edgeScanOperation: TableOperator = {
//    val edgeSelect: TableOperator= Select(edgeTableName, edgeExpr)
//    val fromSelect: TableOperator = Select(fromTableName, fromExpr)
//    val toSelect: TableOperator = Select(toTableName, toExpr)
//    val edgeJoinedOnFrom: TableOperator =
//      InnerJoin(edgeSelect, fromSelect, Column.idColumn, Column.fromIdColumn)
//    val edgeJoinedOnFromAndTo: TableOperator =
//      InnerJoin(edgeJoinedOnFrom, toSelect, Column.idColumn, Column.toIdColumn)
//    edgeJoinedOnFromAndTo
//  }
//
//  val fromScanOperation: TableOperator = {
//    val fromSelect: TableOperator = Select(fromTableName, fromExpr)
//    val fromJoinedOnEdge: TableOperator =
//      InnerJoin(fromSelect, edgeScanOperation, Column.fromIdColumn, Column.idColumn)
//    fromJoinedOnEdge
//  }
//
//  val toScanOperation: TableOperator = {
//    val toSelect: TableOperator = Select(toTableName, toExpr)
//    val toJoinedOnEdge: TableOperator =
//      InnerJoin(toSelect, edgeScanOperation, Column.toIdColumn, Column.idColumn)
//    toJoinedOnEdge
//  }

  children =
    List(edgeBinding, fromBinding, toBinding) //, edgeScanOperation, fromScanOperation, toScanOperation)
}
