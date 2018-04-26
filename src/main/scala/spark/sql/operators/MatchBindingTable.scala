package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.sql.SqlPlanner.BINDING_TABLE_GLOBAL_VIEW
import spark.sql.SqlQuery

case class MatchBindingTable(sparkSession: SparkSession) extends target_api.MatchBindingTable {

  override val bindingTable: BindingTableMetadata = {

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

    SqlBindingTableMetadata(
      sparkSchemaMap = schemaMap,
      sparkBtableSchema = schema,
      btableOps = SqlQuery(resQuery = accessBtableGlobalView)
    )
  }
}
