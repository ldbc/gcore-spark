package planner.exceptions

import common.exceptions.GcoreException

/** An exception thrown by an operation performed within the logical plan. */
abstract class PlannerException(reason: String) extends GcoreException(reason)
