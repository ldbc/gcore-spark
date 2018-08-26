package algebra.target_api

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{Relation, VirtualPathRelation}
import algebra.types.{Graph, PathExpression}
import schema.Catalog

abstract class PathSearch(pathRelation: VirtualPathRelation, graph: Graph, catalog: Catalog)
  extends EntityScan(graph, catalog) {

  val fromTableName: Label = pathRelation.fromRel.labelRelation.asInstanceOf[Relation].label
  val toTableName: Label = pathRelation.toRel.labelRelation.asInstanceOf[Relation].label

  val fromExpr: AlgebraExpression = pathRelation.fromRel.expr
  val toExpr: AlgebraExpression = pathRelation.toRel.expr

  val pathBinding: Reference = pathRelation.ref
  val fromBinding: Reference = pathRelation.fromRel.ref
  val toBinding: Reference = pathRelation.toRel.ref

  val isReachableTest: Boolean = pathRelation.isReachableTest
  val costVarDef: Option[Reference] = pathRelation.costVarDef
  val pathExpression: Option[PathExpression] = pathRelation.pathExpression

  children =
    List(pathBinding, fromBinding, toBinding, fromTableName, toTableName, fromExpr, toExpr) ++
      costVarDef.toList

  override def name: String = s"${super.name} [isReachableTest = $isReachableTest]"
}
