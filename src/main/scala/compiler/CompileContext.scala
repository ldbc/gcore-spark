package compiler

import common.compiler.Context
import org.apache.spark.sql.SparkSession
import schema.Catalog

case class CompileContext(catalog: Catalog, sparkSession: SparkSession) extends Context
