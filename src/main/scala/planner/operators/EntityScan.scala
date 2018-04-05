package planner.operators

import algebra.expressions.{ObjectPattern, Reference}
import algebra.operators.{EdgeRelation, StoredPathRelation, VertexRelation}
import algebra.types.Graph
import planner.trees.{PlannerContext, PlannerTreeNode}

/**
  * [[VertexRelation]]s, [[EdgeRelation]]s and [[StoredPathRelation]]s become [[EntityScan]]s in the
  * logical and physical plan. The scan refers to bringing in the data corresponding to this
  * specific entity (identified by a [[Reference]], an [[ObjectPattern]], its possible endpoints and
  * the [[Graph]] where to look for it) in order to make it available for querying.
  *
  * Further on, the specific backend target will know how to actually perform the scan, as it
  * depends on data model.
  */
abstract class EntityScan(graph: Graph, context: PlannerContext) extends PlannerTreeNode
