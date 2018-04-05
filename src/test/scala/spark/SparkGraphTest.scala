package spark

import algebra.expressions.{Label, PropertyKey}
import algebra.types.{GcoreArray, GcoreBoolean, GcoreInteger, GcoreString}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import schema._

/** Verifies that the [[GraphSchema]] is inferred correctly from the [[GraphData]]. */
class SparkGraphTest extends FunSuite
  with SparkSessionTestWrapper
  with SimpleTestGraph {

  import spark.implicits._

  test("Schema is correctly inferred from graph data") {

    val peopleDf = peopleList.toDF
    val peopleTable = Table(name = Label("people"), data = peopleDf)

    val cityDf = cityList.toDF
    val cityTable = Table(name = Label("city"), data = cityDf)

    val bornInDf = bornInList.toDF
    val bornInTable = Table(name = Label("bornIn"), data = bornInDf)

    val roadDf = roadList.toDF
    val roadTable = Table(name = Label("road"), data = roadDf)

    val travelRouteDf = travelRouteList.toDF
    val travelRouteTable = Table(name = Label("travelRoute"), data = travelRouteDf)

    val graph = new SparkGraph {
      override def graphName: String = "test_graph"
      override def pathData: Seq[Table[DataFrame]] = Seq(travelRouteTable)
      override def vertexData: Seq[Table[DataFrame]] = Seq(peopleTable, cityTable)
      override def edgeData: Seq[Table[DataFrame]] = Seq(bornInTable, roadTable)
      override def edgeRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
      override def storedPathRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
    }

    val expectedVertexSchema = EntitySchema(SchemaMap(Map(
      Label("people") -> SchemaMap(Map(
        PropertyKey("id") -> GcoreInteger,
        PropertyKey("name") -> GcoreString,
        PropertyKey("age") -> GcoreInteger,
        PropertyKey("isAlive") -> GcoreBoolean)),
      Label("city") -> SchemaMap(Map(
        PropertyKey("id") -> GcoreInteger,
        PropertyKey("name") -> GcoreString))
    )))

    val expectedEdgeSchema = EntitySchema(SchemaMap(Map(
      Label("bornIn") -> SchemaMap(Map(
        PropertyKey("id") -> GcoreInteger,
        PropertyKey("fromId") -> GcoreInteger,
        PropertyKey("toId") -> GcoreInteger,
        PropertyKey("hasLeft") -> GcoreBoolean)),
      Label("road") -> SchemaMap(Map(
        PropertyKey("id") -> GcoreInteger,
        PropertyKey("fromId") -> GcoreInteger,
        PropertyKey("toId") -> GcoreInteger))
    )))

    val expectedPathSchema = EntitySchema(SchemaMap(Map(
      Label("travelRoute") -> SchemaMap(Map(
        PropertyKey("id") -> GcoreInteger,
        PropertyKey("fromId") -> GcoreInteger,
        PropertyKey("toId") -> GcoreInteger,
        PropertyKey("edges") -> GcoreArray))
    )))

    assert(graph.vertexSchema == expectedVertexSchema)
    assert(graph.edgeSchema == expectedEdgeSchema)
    assert(graph.pathSchema == expectedPathSchema)
  }
}
