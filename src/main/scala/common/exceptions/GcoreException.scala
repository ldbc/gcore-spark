package common.exceptions

import compiler.Compiler

/** The base class for an [[Exception]] thrown by the [[Compiler]]. */
abstract class GcoreException(reason: String) extends Exception(reason)
