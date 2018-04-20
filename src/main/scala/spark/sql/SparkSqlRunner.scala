package spark.sql

import common.exceptions.UnsupportedOperation
import compiler.RunTargetCodeStage
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.operators.CreateGraph
import planner.trees.PlannerTreeNode

/** Runs the query plan created by the [[SparkSqlPlanner]] on Spark. */
case class SparkSqlRunner(spark: SparkSession) extends RunTargetCodeStage {

  override def runStage(input: PlannerTreeNode): Seq[DataFrame] = {
    val sparkSqlPlanner: SparkSqlPlanner = SparkSqlPlanner(spark)
    input match {
      case createGraph: CreateGraph =>
        val matchClause: PlannerTreeNode = createGraph.matchClause.asInstanceOf[PlannerTreeNode]
        val constructClauses: Seq[PlannerTreeNode] = createGraph.constructClauses

        val data: DataFrame = sparkSqlPlanner.solveBindingTable(matchClause)

        data.show()

        val graphData: Seq[DataFrame] = sparkSqlPlanner.constructGraph(data, constructClauses)
        graphData

      case _ =>
        throw UnsupportedOperation(s"Cannot run query on input type ${input.name}")
    }
  }
}
