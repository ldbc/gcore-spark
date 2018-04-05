package common.exceptions

import compiler.Compiler

/** The base class for an [[Exception]] thrown by the [[Compiler]]. */
abstract class GcoreException(reason: String) extends Exception(reason)

/**
  * Should be thrown for any G-CORE feature that has not been implemented within this interpreter.
  */
case class UnsupportedOperation(reason: String) extends GcoreException(reason)
