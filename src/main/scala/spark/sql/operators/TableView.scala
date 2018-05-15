package spark.sql.operators

import algebra.expressions.Reference
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.sql.SqlQuery

/**
  * Given the [[viewName]] of a temporary table view, infers the
  * [[SqlBindingTableMetadata.schemaMap]] and [[SqlBindingTableMetadata.btableSchema]] from the
  * table schema yielded by the DESCRIBE TABLE command in Spark.
  */
case class TableView(viewName: String, sparkSession: SparkSession)
  extends target_api.TableView(viewName) {

  override val bindingTable: BindingTableMetadata = {

    val globalView: String = s"global_temp.$viewName"

    // Infer binding table's schema from its global view.
    val viewSchema: DataFrame = sparkSession.sql(s"DESCRIBE TABLE $globalView")
    var schema: StructType = new StructType()
    viewSchema
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
      btableOps = SqlQuery(resQuery = globalView)
    )
  }
}
