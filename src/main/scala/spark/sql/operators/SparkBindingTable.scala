package spark.sql.operators

import algebra.expressions.Reference
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import planner.operators.BindingTable

case class SparkBindingTable(schemas: Map[Reference, StructType],
                             btableOps: SqlQuery) extends BindingTable {
  override type SchemaType = StructType
  override type QueryOperand = SqlQuery

  override val schemaMap: Map[Reference, StructType] = schemas
  override val btableOp: SqlQuery = btableOps

  def solveBtableOps(sparkSession: SparkSession): DataFrame = {
    btableOp.prologue.foreach(sparkSession.sql)
    val result: DataFrame = sparkSession.sql(btableOp.resQuery)
    btableOp.epilogue.foreach(sparkSession.sql)
    result
  }

  def showSchemas(): Unit = {
    schemaMap.foreach(kv => {
      println(kv._1)
      kv._2.printTreeString()
    })
  }
}
