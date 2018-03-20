package planner.exceptions

import common.exceptions.GcoreException

abstract class PlannerException(reason: String) extends GcoreException(reason)
