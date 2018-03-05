package algebra.exceptions

case class UnsupportedOperation(reason: String) extends IrException(reason)
