package algebra.operators

import algebra.expressions.{LabelRemove, PropertyRemove, PropertySet}
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
