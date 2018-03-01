package api.exceptions

import api.compiler.Compiler

/** The base class for an [[Exception]] thrown by the [[Compiler]]. */
abstract class GcoreException(message: String) extends Exception(message)
