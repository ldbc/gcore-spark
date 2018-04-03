package spark.exceptions

import common.exceptions.GcoreException

abstract class SparkException(reason: String) extends GcoreException(reason)

case class UnsupportedOperation(reason: String) extends SparkException(reason)
