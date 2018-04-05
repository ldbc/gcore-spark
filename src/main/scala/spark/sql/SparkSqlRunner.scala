package spark.sql

import compiler.RunTargetCodeStage
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.trees.PlannerTreeNode

/** Runs the query plan created by the [[SparkSqlPlanner]] on Spark. */
case class SparkSqlRunner(spark: SparkSession) extends RunTargetCodeStage {

  override def runStage(input: PlannerTreeNode): Unit = {
    val btable: DataFrame = SparkSqlPlanner(spark).createBindingTable(input)
    btable.show()
  }
}
