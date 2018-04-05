package algebra.exceptions

import common.exceptions.GcoreException

/** An exception thrown by an operation performed within the algebraic tree. */
abstract class AlgebraException(reason: String) extends GcoreException(reason)
