package spark

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.Table

case class JsonGraphSource(spark: SparkSession) extends GraphSource(spark) {

  override def loadGraph(graphConfig: GraphJsonConfig): SparkGraph = {

    new SparkGraph {
      override def graphName: String = graphConfig.graphName

      override def pathData: Seq[Table[DataFrame]] = loadData(graphConfig.pathFiles)

      override def vertexData: Seq[Table[DataFrame]] = loadData(graphConfig.vertexFiles)

      override def edgeData: Seq[Table[DataFrame]] = loadData(graphConfig.edgeFiles)

      private def loadData(dataFiles: Seq[Path]): Seq[Table[DataFrame]] =
        dataFiles.map(
          filePath =>
            Table(name = filePath.getFileName.toString, data = spark.read.json(filePath.toString)))
    }
  }
}
