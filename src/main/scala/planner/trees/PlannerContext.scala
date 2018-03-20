package planner.trees

import common.compiler.Context
import schema.GraphDb

case class PlannerContext(graphDb: GraphDb) extends Context
