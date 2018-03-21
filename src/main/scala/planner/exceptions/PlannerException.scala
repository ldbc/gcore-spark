package planner.exceptions

import common.exceptions.GcoreException

abstract class PlannerException(reason: String) extends GcoreException(reason)

case class UnsupportedOperation(reason: String) extends GcoreException(reason)
