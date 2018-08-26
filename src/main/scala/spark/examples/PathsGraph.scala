package spark.examples

import algebra.expressions.Label
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.EntitySchema.LabelRestrictionMap
import schema.{SchemaMap, Table}
import spark.SparkGraph

case class PathsGraph(spark: SparkSession) extends SparkGraph {

  val A = Blue(0, "A")
  val B = Blue(1, "B")
  val C = Blue(2, "C")
  val D = Blue(3, "D")
  val E = Blue(4, "E")
  val F = Blue(5, "F")
  val G = Blue(6, "G")
  val H = Blue(7, "H")

  val AB = Knows(10, 0, 1)
  val AE = Knows(11, 0, 4)
  val BC = Knows(12, 1, 2)
  val BE = Knows(13, 1, 4)
  val CD = Knows(14, 2, 3)
  val DE = Knows(15, 3, 4)
  val DF = Knows(16, 3, 5)
  val ED = Knows(17, 4, 3)
  val EG = Knows(18, 4, 6)
  val GA = Knows(19, 6, 0)

  val AF = RelatedTo(20, 0, 5)
  val GD = RelatedTo(22, 6, 3)

  import spark.implicits._

  override def graphName: String = "paths_graph"

  override def vertexData: Seq[Table[DataFrame]] = Seq(
    Table(
      name = Label("Blue"),
      data = Seq(A, B, C, D, E, F, G, H).toDF)
  )

  override def edgeData: Seq[Table[DataFrame]] = Seq(
    Table(
      name = Label("knows"),
      data = Seq(AB, AE, BC, BE, CD, DE, DF, ED, EG, GA).toDF),
    Table(
      name = Label("relatedTo"),
      data = Seq(AF, GD).toDF)
  )

  override def pathData: Seq[Table[DataFrame]] = Seq.empty

  override def edgeRestrictions: LabelRestrictionMap = SchemaMap(Map(
    Label("knows") -> (Label("Blue"), Label("Blue")),
    Label("relatedTo") -> (Label("Blue"), Label("Blue"))
  ))

  override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty
}

sealed case class Blue(id: Int, name: String)
sealed case class Grey(id: Int, name: String)
sealed case class Knows(id: Int, fromId: Int, toId: Int)
sealed case class RelatedTo(id: Int, fromId: Int, toId: Int)
