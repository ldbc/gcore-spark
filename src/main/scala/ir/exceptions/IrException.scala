package ir.exceptions

import api.exceptions.GcoreException

final case class IrException(message: String) extends GcoreException(message)
