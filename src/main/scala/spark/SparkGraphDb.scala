package spark

import org.apache.spark.sql.DataFrame
import schema.{GraphDb, PathPropertyGraph}

object SparkGraphDb extends GraphDb[DataFrame] {

  override def allGraphs: Seq[PathPropertyGraph[DataFrame]] = ???
}
