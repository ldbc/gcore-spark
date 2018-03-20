package spark.sql

import compiler.RunTargetCodeStage
import org.apache.spark.sql.{DataFrame, SparkSession}
import planner.trees.{PlannerContext, PlannerTreeNode, TargetTreeNode}
import spark.sql.operators.SparkBindingTable

case class SparkSqlRunner(spark: SparkSession, plannerContext: PlannerContext)
  extends RunTargetCodeStage {

  override def runStage(input: PlannerTreeNode): Unit = {
    val sparkSqlPlanner: SparkSqlPlanner = SparkSqlPlanner(spark, input, plannerContext)
    val targetTree: TargetTreeNode = sparkSqlPlanner.rewriteTree(input).asInstanceOf[TargetTreeNode]
    val targetTreeBtable: SparkBindingTable =
      targetTree.bindingTable.asInstanceOf[SparkBindingTable]
    val btable: DataFrame = targetTreeBtable.solveBtableOps(spark)
    btable.show()
    targetTreeBtable.showSchemas()
  }
}
