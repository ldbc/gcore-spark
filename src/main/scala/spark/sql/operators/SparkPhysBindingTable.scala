package spark.sql.operators

import algebra.expressions.Reference
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import planner.target_api.{BindingTable, PhysBindingTable}
import spark.sql.SparkSqlPlanner.BINDING_TABLE_GLOBAL_VIEW

case class SparkPhysBindingTable(sparkSession: SparkSession) extends PhysBindingTable {

  override val bindingTable: BindingTable = {

    val accessBtableGlobalView: String = s"global_temp.$BINDING_TABLE_GLOBAL_VIEW"

    // Infer binding table's schema from its global view.
    val globalBtableSchema: DataFrame = sparkSession.sql(s"DESCRIBE TABLE $accessBtableGlobalView")
    var schema: StructType = new StructType()
    globalBtableSchema
      .collect()
      .foreach(attributes =>
        schema = schema.add(attributes(0).asInstanceOf[String], attributes(1).asInstanceOf[String]))

    val schemaMap: Map[Reference, StructType] =
      schema
        .map(field => {
          val fieldNameTokens: Array[String] = field.name.split("\\$")
          val refName: String = fieldNameTokens.head
          Reference(refName) -> field
        })
        .groupBy(_._1)
        .map {
          case (ref, seqOfTuples) => (ref, StructType(seqOfTuples.map(_._2)))
        }

    SparkBindingTable(
      sparkSchemaMap = schemaMap,
      sparkBtableSchema = schema,
      btableOps = SqlQuery(resQuery = accessBtableGlobalView)
    )
  }
}
