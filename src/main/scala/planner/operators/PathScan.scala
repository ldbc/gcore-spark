package planner.operators

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{Relation, StoredPathRelation}
import algebra.types.{Graph, PathQuantifier}
import planner.trees.PlannerContext

case class PathScan(pathRelation: StoredPathRelation, graph: Graph, context: PlannerContext)
  extends EntityScan(graph, context) {

  val pathTableName: Label = pathRelation.labelRelation.asInstanceOf[Relation].label
  val fromTableName: Label = pathRelation.fromRel.labelRelation.asInstanceOf[Relation].label
  val toTableName: Label = pathRelation.toRel.labelRelation.asInstanceOf[Relation].label

  val pathExpr: AlgebraExpression = pathRelation.expr
  val fromExpr: AlgebraExpression = pathRelation.fromRel.expr
  val toExpr: AlgebraExpression = pathRelation.toRel.expr

  val pathBinding: Reference = pathRelation.ref
  val fromBinding: Reference = pathRelation.fromRel.ref
  val toBinding: Reference = pathRelation.toRel.ref

  val isReachableTest: Boolean = pathRelation.isReachableTest
  val costVarDef: Option[Reference] = pathRelation.costVarDef
  val quantifier: Option[PathQuantifier] = pathRelation.quantifier

  children =
    List(pathBinding, fromBinding, toBinding, pathTableName, fromTableName, toTableName,
      pathExpr, fromExpr, toExpr) ++ costVarDef.toList ++ quantifier.toList

  override def name: String = s"${super.name} [isReachableTest = $isReachableTest]"
}
