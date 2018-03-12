package parser.trees

import common.compiler.Context
import schema.GraphDb

case class ParseContext(graphDb: GraphDb) extends Context
