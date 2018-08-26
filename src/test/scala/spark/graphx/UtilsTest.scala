package spark.graphx

import algebra.operators.Column.{EDGE_SEQ_COL, FROM_ID_COL, ID_COL, TO_ID_COL}
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite
import spark.SparkSessionTestWrapper
import spark.graphx.ShortestPaths.{Distance, EdgeId}
import spark.graphx.Utils.COST_COL

class UtilsTest extends FunSuite with SparkSessionTestWrapper {

  test("Create virtual path data as a DataFrame") {
    import spark.implicits._

    val fromData: DataFrame = Seq(1, 2, 3, 4, 5, 6).toDF(ID_COL.columnName)
    val toData: DataFrame = Seq(1, 2, 3, 4, 5, 6).toDF(ID_COL.columnName)
    val edgeData: DataFrame =
      Seq(
        (1, 2, 102), (1, 5, 105), (2, 3, 203), (2, 5, 205),
        (3, 4, 304), (4, 5, 405), (4, 6, 406), (5, 2, 502))
        .toDF(FROM_ID_COL.columnName, TO_ID_COL.columnName, ID_COL.columnName)

    val expectedPathData: Seq[Path] = Seq(
      Path(1, 2, 1, Seq(102)), Path(1, 3, 2, Seq(102, 203)), Path(1, 4, 3, Seq(102, 203, 304)),
      Path(1, 5, 1, Seq(105)), Path(1, 6, 4, Seq(102, 203, 304, 406)),

      Path(2, 3, 1, Seq(203)), Path(2, 4, 2, Seq(203, 304)), Path(2, 5, 1, Seq(205)),
      Path(2, 6, 3, Seq(203, 304, 406)),

      Path(3, 2, 3, Seq(304, 405, 502)), Path(3, 4, 1, Seq(304)), Path(3, 5, 2, Seq(304, 405)),
      Path(3, 6, 2, Seq(304, 406)),

      Path(4, 2, 2, Seq(405, 502)), Path(4, 3, 3, Seq(405, 502, 203)), Path(4, 5, 1, Seq(405)),
      Path(4, 6, 1, Seq(406)),

      Path(5, 2, 1, Seq(502)), Path(5, 3, 2, Seq(502, 203)), Path(5, 4, 3, Seq(502, 203, 304)),
      Path(5, 6, 4, Seq(502, 203, 304, 406))
    )

    val expectedHeader: Seq[String] =
      Seq(FROM_ID_COL.columnName, TO_ID_COL.columnName, COST_COL, EDGE_SEQ_COL.columnName)

    val actualPathDataFrame: DataFrame = Utils.createPathData(edgeData, fromData, toData, spark)
    val actualPathData: Seq[Path] =
      actualPathDataFrame
        .collect
        .map {
          case Row(fromId: VertexId, toId: VertexId, pathCost: Distance, edges: Seq[EdgeId]) =>
            Path(fromId, toId, pathCost, edges)
        }

    compareHeaders(expectedHeader, actualPathDataFrame)
    assert(actualPathData.size == expectedPathData.size)
    assert(actualPathData.toSet == expectedPathData.toSet)
  }

  sealed case class Path(fromId: VertexId, toId: VertexId, pathCost: Distance, edges: Seq[EdgeId])
}
