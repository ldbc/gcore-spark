package algebra.operators

import algebra.expressions.{AlgebraExpression, LabelAssignments, PropertyRef, Reference}
import common.compiler.Context

/**
  * An operation on a graph that updates its structure, such as labels or properties of its
  * entities.
  */
abstract class GraphUpdate extends GcoreOperator {

  override def checkWithContext(context: Context): Unit = {}
}

/** A wrapper over a sequence of [[PropertySet]] clauses. */
case class SetClause(propSets: Seq[PropertySet]) extends GraphUpdate {
  children = propSets
}

/** A wrapper over a sequence of [[PropertyRemove]] and [[LabelRemove]] clauses. */
case class RemoveClause(propRemoves: Seq[PropertyRemove], labelRemoves: Seq[LabelRemove])
  extends GraphUpdate {

  children = propRemoves ++ labelRemoves
}

/**
  * Updates the graph by creating a new property for a variable with a value given as an
  * [[AlgebraExpression]].
  */
case class PropertySet(propertyRef: PropertyRef, expr: AlgebraExpression) extends GraphUpdate {
  children = List(propertyRef, expr)
}

/** Updates the graph by removing a property from a variable. */
case class PropertyRemove(propertyRef: PropertyRef) extends GraphUpdate {
  children = List(propertyRef)
}

/** Updates the graph by removing a label from a variable. */
case class LabelRemove(ref: Reference, labelAssignments: LabelAssignments) extends GraphUpdate {
  children = List(ref, labelAssignments)
}
