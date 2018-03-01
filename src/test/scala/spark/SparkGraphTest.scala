package spark

import ir.algebra.expressions.{Label, PropertyKey}
import ir.algebra.types.{TypeArray, TypeBoolean, TypeInteger, TypeString}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import schema._

/** Verifies that the [[GraphSchema]] is inferred correctly from the [[GraphData]]. */
class SparkGraphTest extends FunSuite
  with SparkSessionTestWrapper
  with TestGraphWrapper {

  import spark.implicits._

  test("Schema is correctly inferred from graph data") {

    val peopleDf = peopleList.toDF
    val peopleTable = Table(name = "people", data = peopleDf)

    val cityDf = cityList.toDF
    val cityTable = Table(name = "city", data = cityDf)

    val bornInDf = bornInList.toDF
    val bornInTable = Table(name = "bornIn", data = bornInDf)

    val roadDf = roadList.toDF
    val roadTable = Table(name = "road", data = roadDf)

    val travelRouteDf = travelRouteList.toDF
    val travelRouteTable = Table(name = "travelRoute", data = travelRouteDf)

    val graph = new SparkGraph {
      override def graphName: String = "test_graph"
      override def pathData: Seq[Table[DataFrame]] = Seq(travelRouteTable)
      override def vertexData: Seq[Table[DataFrame]] = Seq(peopleTable, cityTable)
      override def edgeData: Seq[Table[DataFrame]] = Seq(bornInTable, roadTable)
    }

    val expectedVertexSchema = EntitySchema(SchemaMap(Map(
      Label("people") -> SchemaMap(Map(
        PropertyKey("id") -> TypeInteger(),
        PropertyKey("name") -> TypeString(),
        PropertyKey("age") -> TypeInteger(),
        PropertyKey("isAlive") -> TypeBoolean())),
      Label("city") -> SchemaMap(Map(
        PropertyKey("id") -> TypeInteger(),
        PropertyKey("name") -> TypeString()))
    )))

    val expectedEdgeSchema = EntitySchema(SchemaMap(Map(
      Label("bornIn") -> SchemaMap(Map(
        PropertyKey("id") -> TypeInteger(),
        PropertyKey("fromId") -> TypeInteger(),
        PropertyKey("toId") -> TypeInteger(),
        PropertyKey("hasLeft") -> TypeBoolean())),
      Label("road") -> SchemaMap(Map(
        PropertyKey("id") -> TypeInteger(),
        PropertyKey("fromId") -> TypeInteger(),
        PropertyKey("toId") -> TypeInteger()))
    )))

    val expectedPathSchema = EntitySchema(SchemaMap(Map(
      Label("travelRoute") -> SchemaMap(Map(
        PropertyKey("id") -> TypeInteger(),
        PropertyKey("edges") -> TypeArray()))
    )))

    assert(graph.vertexSchema == expectedVertexSchema)
    assert(graph.edgeSchema == expectedEdgeSchema)
    assert(graph.pathSchema == expectedPathSchema)
  }
}
