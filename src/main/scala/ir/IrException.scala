package ir

final case class IrException(private val message: String = "",
                             private val cause: Throwable = None.orNull)
  extends Exception
