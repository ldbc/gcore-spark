package parser.exceptions

import common.exceptions.GcoreException

/** An exception thrown during the parsing stage. */
abstract class ParserException(message: String) extends GcoreException(message)

/**
  * An exception thrown during the loading of the G-CORE language component by the Spoofax parser.
  */
case class LanguageLoadException(message: String) extends ParserException(message)

case class QueryParseException(message: String) extends ParserException(message)
