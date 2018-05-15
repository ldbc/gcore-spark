package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api.BindingTableMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import spark.sql.SqlQuery

/**
  * Metadata of a binding table, where the schema is stored as a [[StructType]] and the operation
  * on the binding table is a [[SqlQuery]].
  */
case class SqlBindingTableMetadata(sparkSchemaMap: Map[Reference, StructType],
                                   sparkBtableSchema: StructType,
                                   btableOps: SqlQuery) extends BindingTableMetadata {

  override type SchemaType = StructType
  override type QueryOperand = SqlQuery

  override val schemaMap: Map[Reference, StructType] = sparkSchemaMap
  override val btableSchema: StructType = sparkBtableSchema
  override val btable: SqlQuery = btableOps

  /**
    * Runs the SQL queries in [[btable]]. First runs the queries in the [[SqlQuery.prologue]], then
    * the [[SqlQuery.resQuery]], then the queries in the [[SqlQuery.epilogue]]. The result is the
    * [[DataFrame]] obtained by running the [[SqlQuery.resQuery]].
    */
  def solveBtableOps(sparkSession: SparkSession): DataFrame = {
    btable.prologue.foreach(sparkSession.sql)
    val result: DataFrame = sparkSession.sql(btable.resQuery)
    btable.epilogue.foreach(sparkSession.sql)
    result
  }

  /**
    * Pretty prints the schemas of each variable in the [[schemaMap]], then that of the binding
    * table's.
    */
  def showSchemas(): Unit = {
    schemaMap.foreach(kv => {
      println(kv._1)
      kv._2.printTreeString()
    })

    println("Btable schema:")
    btableSchema.printTreeString()
  }
}
