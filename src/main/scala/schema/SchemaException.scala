package schema

import common.exceptions.GcoreException

case class SchemaException(message: String) extends GcoreException(message)
