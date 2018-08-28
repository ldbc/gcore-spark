package algebra.operators
import algebra.expressions.AlgebraExpression
import algebra.types.{ConstructPattern, NamedGraph, QueryGraph}
import common.compiler.Context

/** A construct-like operator that participates in the construct sub-query of a G-CORE query. */
abstract class ConstructLike extends GcoreOperator {
  override def checkWithContext(context: Context): Unit = {}
}

/**
  * The top-most construct clause of the query that dictates how the resulting graph should be
  * built. The new graph can result from the [[GraphUnion]] of [[NamedGraph]]s or [[QueryGraph]]s,
  * unioned with graphs resulting from [[CondConstructs]]s. Additionally, the resulting graph
  * can be updated with [[SetClause]]s and [[RemoveClause]]s.
  */
case class ConstructClause(graphs: GraphUnion, condConstructs: CondConstructs,
                           setClause: SetClause, removeClause: RemoveClause) extends ConstructLike {

  children = List(graphs, condConstructs, setClause, removeClause)
}

/** A wrapper over a sequence of [[CondConstructClause]]s. */
case class CondConstructs(condConstructs: Seq[CondConstructClause]) extends ConstructLike {
  children = condConstructs
}

/**
  * The most basic construction clause, that specifies a [[ConstructPattern]] for the binding table
  * and a WHEN condition for additional filtering on the table.
  */
case class CondConstructClause(constructPattern: ConstructPattern, when: AlgebraExpression)
  extends ConstructLike {

  children = List(constructPattern, when)
}
