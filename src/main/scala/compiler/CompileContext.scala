package compiler

import common.compiler.Context
import org.apache.spark.sql.SparkSession
import schema.GraphDb

case class CompileContext(graphDb: GraphDb, sparkSession: SparkSession) extends Context
