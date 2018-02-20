package ir

final case class IrException(val message: String,
                             val cause: Throwable = scala.None.orNull) extends Exception {
  override def toString: String = message
}
