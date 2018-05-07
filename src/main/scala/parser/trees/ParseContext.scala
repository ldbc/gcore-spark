package parser.trees

import common.compiler.Context
import schema.Catalog

case class ParseContext(catalog: Catalog) extends Context
