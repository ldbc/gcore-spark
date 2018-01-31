package ir

final case class IrException(private val message: String = "",
                             private val cause: Throwable = scala.None.orNull)
  extends Exception
