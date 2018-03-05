package spark

import common.exceptions.GcoreException

case class SparkException(message: String) extends GcoreException(message)
