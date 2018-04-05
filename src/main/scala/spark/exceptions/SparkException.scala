package spark.exceptions

import common.exceptions.GcoreException

/**
  * An exception thrown by an operation performed within the evaluation of a G-CORE query on Spark.
  */
abstract class SparkException(reason: String) extends GcoreException(reason)
